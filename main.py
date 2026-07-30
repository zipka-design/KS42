import os
import uuid
import time

from google import genai
from dotenv import load_dotenv
import json
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import Message, Channel
from telethon.errors import SessionPasswordNeededError, RpcCallFailError
from telethon.tl.custom import Button
import base64
from typing import List, Optional
from pydantic import BaseModel, Field
from thefuzz import fuzz, process

def log(tag, msg):
    """Логирование с таймстампом [HH:MM:SS] [tag] message"""
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{tag}] {msg}")

def load_json(path):
    if not os.path.exists(path):
        save_json({}, path)
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)
def save_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_txt(path):
    if not os.path.exists(path):
        save_txt("", path)
        return ""
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
def save_txt(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data)

class Output(BaseModel):
    """Схема ответа Gemini: текст поста + флаг ожидания + вопрос админу"""
    post_text: str = Field(default="", description="текст поста")
    wait: bool = Field(description="True — пост содержит недосказанность и НЕ отправляется (ни в модерацию, ни в канал). False — пост готов и отправляется на модерацию.")
    ask_about: str = Field(default="", description="если ты нашел новое слово, и даже при вызове инструмента тв не знаешь что оно означает, спроси меня что это такое, и тогда остальные поля оставь в False и ''")
load_dotenv()

# ID админа для модерации постов
ADMIN_ID = int(os.getenv("ADMIN_ID"))
# ID канала для публикации одобренных постов
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

# Telegram-клиент бота (модерация, отправка в канал)
telegram_bot = TelegramClient('bot', os.getenv("TELEGRAM_API_ID"), os.getenv("TELEGRAM_API_HASH"))
# Telegram-клиент пользователя (чтение каналов-источников)
telegram_user = TelegramClient('user', os.getenv("TELEGRAM_API_ID"), os.getenv("TELEGRAM_API_HASH"))
# Клиент Gemini AI для генерации постов
gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Состояния диалогов с админом (ожидание ввода: wait_link, regen:id)
bot_states = {}
# Буферы альбомов (групповых фото/видео) — keyed by grouped_id
album_buffers = {}
# Задержка перед обработкой альбома (сек) — ждём все фото
ALBUM_DELAY = 2.0
# Папка для временных файлов (JSON-состояния, скачанные медиа)
TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)

# Режим загрузки медиа: 0 = base64 в запросе, 1 = загрузка через files.upload (URI)
TYPE_TO_SEND = 1
# Модель Gemini для генерации постов
MODEL = "gemini-3.1-flash-lite"

test = True

async def upload_file_and_wait(tmp_path, timeout=120):
    """Загружает файл в Gemini и ждёт пока станет ACTIVE"""
    uploaded = gemini_client.files.upload(file=tmp_path)
    deadline = time.time() + timeout
    while uploaded.state.name != "ACTIVE":
        if time.time() > deadline:
            raise TimeoutError(f"Файл {tmp_path} не стал ACTIVE за {timeout}с")
        await asyncio.sleep(2)
        uploaded = gemini_client.files.get(name=uploaded.name)
    return uploaded

async def _call_gemini(input_data, previous_id=None, max_retries=3):
    """Вызывает Gemini с таймаутом 60 с; повторяет при превышении."""
    for attempt in range(1, max_retries + 1):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    gemini_client.interactions.create,
                    model=MODEL,
                    input=input_data,
                    response_format={
                        "type": "text",
                        "mime_type": "application/json",
                        "schema": Output.model_json_schema()
                    },
                    previous_interaction_id=previous_id,
                    tools=[get_additional_info_tool]
                ),
                timeout=60
            )
        except (TimeoutError, asyncio.TimeoutError):
            log("ai", f"Таймаут (попытка {attempt}/{max_retries}), повтор...")
    raise TimeoutError(f"Gemini не ответил после {max_retries} попыток")

# Очередь задач AI по chat_id — чтобы не обрабатывать один канал параллельно
calls_to_ai = {}

async def prepare_a_news_item(chat_id, event):
    """Очередь: если для chat_id уже идёт запрос — ждём, потом запускаем новый"""
    if chat_id in calls_to_ai:
        log("queue", f"Ждём завершения предыдущего запроса для chat_id={chat_id}")
        await calls_to_ai[chat_id]

    task = asyncio.create_task(_run_news_item(event))
    calls_to_ai[chat_id] = task
    try:
        await task
    finally:
        calls_to_ai.pop(chat_id, None)

async def _run_news_item(event):
    # Загружаем инструкцию для Gemini из instruction.md
    INSTRUCTION = load_txt("instruction.md")

    # Определяем чат и название канала-источника
    if isinstance(event, list):
        # Альбом: берём chat из первого фото
        chat = await event[0].get_chat()
        name = chat.title
    else:
        # Одиночное сообщение
        chat = await event.get_chat()
        name = getattr(chat, "title", "")

    # ID канала как строка — используется для папки и имён JSON
    id_request = str(chat.id)
    os.makedirs(os.path.join(TEMP_DIR, id_request), exist_ok=True)

    # Путь к pending JSON — хранит контекст между сообщениями (если wait=True)
    pending_path = os.path.join(TEMP_DIR, id_request, f"{id_request}.json")
    # Загружаем ранее сохранённый контекст (если есть — значит было wait=True)
    pending_data = load_json(pending_path)
    prepare_request = pending_data.get("request_to_save", [])
    media_ids = pending_data.get("media_ids", [])

    # Логируем содержимое прочитанного файла
    if prepare_request:
        log("file", f"Прочитан {pending_path}:\n{json.dumps(prepare_request, ensure_ascii=False, indent=2)}")
    else:
        log("file", f"Файл {pending_path} пуст или не существует")

    # Если pending пуст — начинаем новый запрос: инструкция + имя канала
    if not prepare_request:
        prepare_request = [{
            "type": "text",
            "text": INSTRUCTION,
        },
        {
            "type": "text",
            "text": "from channel: " + name,
        }]
    if not media_ids:
        media_ids = []

    url_to_msg = {"msg": "", "url": ""}

    _name = getattr(chat, 'title', None) or ' '.join(
        filter(None, [getattr(chat, 'first_name', ''), getattr(chat, 'last_name', '')])) or 'Без имени'
    username = getattr(chat, "username", None)
    if not username and getattr(chat, "usernames", None):
        username = getattr(chat.usernames[0], "username", None)

    url_to_msg["msg"] = f"Из канала {_name}"
    msg_id = event[0].message.id if isinstance(event, list) else event.message.id
    url_to_msg["url"] =  f"https://t.me/{username}/{msg_id}"

    if chat.id == (await telegram_bot.get_me()).id or chat.id == ADMIN_ID or chat.id == CHANNEL_ID:
        url_to_msg = {"msg": "", "url": ""}

    # --- Обработка контента из event (текст, фото, видео, аудио) ---
    if isinstance(event, list):
        # Альбом: обходим каждое сообщение в группе
        for i in event:
            date = i.message.date

            if i.message.message:
                message = i.message.message
                prepare_request.append(
                    {
                        "type": "text",
                        "text": "message: " + message,
                    }
                )
                log("msg", f"Текст: {message}...")
            if i.message.photo:
                # Фото: загружаем как base64 или через URI
                if TYPE_TO_SEND == 0:
                    photo_bytes = await i.message.download_media(bytes)
                    image_b64 = base64.b64encode(photo_bytes).decode("utf-8")
                    prepare_request.append({
                            "type": "image",
                            "data": image_b64,
                            "mime_type": "image/jpeg"
                    })
                else:
                    # URI-режим: скачиваем временно, загружаем в Gemini, получаем URI
                    media_path = os.path.join(TEMP_DIR, id_request, f"tmp_photo_{uuid.uuid4().hex}.jpg")
                    await i.message.download_media(file=media_path)
                    uploaded_file = await upload_file_and_wait(media_path)
                    prepare_request.append({
                        "type": "image",
                        "uri": uploaded_file.uri,
                        "mime_type": uploaded_file.mime_type
                    })
                    media_ids.append(media_path)
                log("media", "Фото")
            if i.message.video or i.message.video_note:
                # Видео: аналогично фото
                if TYPE_TO_SEND == 0:
                    video_bytes = await i.message.download_media(bytes)
                    video_b64 = base64.b64encode(video_bytes).decode("utf-8")
                    prepare_request.append({
                        "type": "video",
                        "data": video_b64,
                        "mime_type": "video/mp4"
                    })
                else:
                    media_path = os.path.join(TEMP_DIR, id_request, f"tmp_video_{uuid.uuid4().hex}.mp4")
                    await i.message.download_media(file=media_path)
                    uploaded_file = await upload_file_and_wait(media_path)
                    prepare_request.append({
                        "type": "video",
                        "uri": uploaded_file.uri,
                        "mime_type": uploaded_file.mime_type
                    })
                    media_ids.append(media_path)
                log("media", "Видео")
            if i.message.voice or i.message.audio:
                # Аудио: аналогично фото
                if TYPE_TO_SEND == 0:
                    audio_bytes = await i.message.download_media(bytes)
                    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
                    prepare_request.append({
                        "type": "audio",
                        "data": audio_b64,
                        "mime_type": "audio/ogg"
                    })
                else:
                    log("upload", "Загрузка аудио")
                    media_path = os.path.join(TEMP_DIR, id_request, f"tmp_audio_{uuid.uuid4().hex}.ogg")
                    await i.message.download_media(file=media_path)
                    uploaded_file = await upload_file_and_wait(media_path)
                    prepare_request.append({
                        "type": "audio",
                        "uri": uploaded_file.uri,
                        "mime_type": uploaded_file.mime_type
                    })
                    media_ids.append(media_path)
                log("media", "Аудио")
    else:
        # Одиночное сообщение: обрабатываем аналогично, но без цикла
        date = event.message.date

        if event.message.message:
            message = event.message.message
            prepare_request.append({
                    "type": "text",
                    "text": "message: " + message
                })
            log("msg", f"Текст: {message}...")

        if event.message.photo:
            if TYPE_TO_SEND == 0:
                photo_bytes = await event.message.download_media(bytes)
                image_b64 = base64.b64encode(photo_bytes).decode("utf-8")
                prepare_request.append({
                        "type": "image",
                        "data": image_b64,
                        "mime_type": "image/jpeg"
                })
            else:
                media_path = os.path.join(TEMP_DIR, id_request, f"tmp_photo_{uuid.uuid4().hex}.jpg")
                await event.message.download_media(file=media_path)
                uploaded_file = await upload_file_and_wait(media_path)
                prepare_request.append({
                    "type": "image",
                    "uri": uploaded_file.uri,
                    "mime_type": uploaded_file.mime_type
                })
                media_ids.append(media_path)
            log("media", "Фото")

        if event.message.video or event.message.video_note:
            if TYPE_TO_SEND == 0:
                video_bytes = await event.message.download_media(bytes)
                video_b64 = base64.b64encode(video_bytes).decode("utf-8")
                prepare_request.append({
                    "type": "video",
                    "data": video_b64,
                    "mime_type": "video/mp4"
                })
            else:
                media_path = os.path.join(TEMP_DIR, id_request, f"tmp_video_{uuid.uuid4().hex}.mp4")
                await event.message.download_media(file=media_path)
                uploaded_file = await upload_file_and_wait(media_path)
                prepare_request.append({
                    "type": "video",
                    "uri": uploaded_file.uri,
                    "mime_type": uploaded_file.mime_type
                })
                media_ids.append(media_path)
            log("media", "Видео")

        if event.message.voice or event.message.audio:
            if TYPE_TO_SEND == 0:
                audio_bytes = await event.message.download_media(bytes)
                audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
                prepare_request.append({
                    "type": "audio",
                    "data": audio_b64,
                    "mime_type": "audio/ogg"
                })
            else:
                media_path = os.path.join(TEMP_DIR, id_request, f"tmp_audio_{uuid.uuid4().hex}.ogg")
                await event.message.download_media(file=media_path)
                uploaded_file = await upload_file_and_wait(media_path)
                prepare_request.append({
                    "type": "audio",
                    "uri": uploaded_file.uri,
                    "mime_type": uploaded_file.mime_type
                })
                media_ids.append(media_path)
            log("media", "Аудио")

    # Ссылки на контекст для сохранения и для отправки в AI
    request_to_save = prepare_request  # что запишем в JSON
    to_ai = prepare_request            # что отправим в Gemini

    log("ai", f"Запрос ({len(to_ai)} частей)")

    # --- Цикл вызовов Gemini (с обработкой function calls) ---
    previous_id = None  # ID предыдущего взаимодействия для цепочки
    while True:
        log("ai", "Цикл вызовов")
        # Отправляем запрос в Gemini с инструментами (таймаут 60 с, повтор при ошибке)
        interaction = await _call_gemini(to_ai, previous_id)
        # Обрабатываем вызовы инструментов (get_additional_info и тд)
        function_results = []
        for step in interaction.steps:
            if step.type == "function_call":
                result = available_functions[step.name](**step.arguments)
                log("tool", f"{step.name}({step.arguments}) → {result}")
                # Формируем результат для следующего шага AI
                t = {
                    "type": "function_result",
                    "name": step.name,
                    "call_id": step.id,
                    "result": [{"type": "text", "text": json.dumps(result)}],
                }
                function_results.append(t)
                # Добавляем в контекст для сохранения в JSON
                request_to_save.append({
                    "type": "text",
                    "text": f"[инструмент {step.name}]: запрос \"{step.arguments}\" → результат: {json.dumps(result, ensure_ascii=False)}"
                })

        # Если не было вызовов инструментов — AI готовит финальный ответ
        if not function_results:
            break

        # Отправляем результаты инструментов обратно в AI
        to_ai = function_results
        previous_id = interaction.id  # сохраняем ID для цепочки

    # Парсим JSON-ответ Gemini в модель Output
    result = Output.model_validate_json(interaction.output_text)

    if result.ask_about:
        new_id_request = f"{id_request}_{uuid.uuid4().hex}"
        request_to_save.append({
            "type": "text",
            "text": f"вопрос: " + result.ask_about
        })

        # Кнопки: ответить или отмена (удаление)
        buttons = [
            [
                Button.inline("Ответить", data=f"answer:{new_id_request}"),
                Button.inline("Отмена", data=f"cancel:{new_id_request}"),
            ],
            [
                Button.inline("✅ Опубликовать", data=f"publish:{new_id_request}"),
            ]
        ]
        # Отправляем медиа отдельными сообщениями после текста
        for media_path in media_ids:
            if os.path.exists(media_path):
                await telegram_bot.send_file(ADMIN_ID, media_path)
                log("media", f"Медиа отправлено: {media_path}")

        # Отправляем пост админу на модерацию
        source_line = ""
        if url_to_msg.get("url"):
            source_line = f"[{url_to_msg.get('msg', '')}]({url_to_msg['url']})\n\n"
        text_part = "Текста поста нет" if not result.post_text else f"Пост: {result.post_text}"
        sent = await telegram_bot.send_message(
            ADMIN_ID,
            f"{source_line}{text_part}\nВопрос: {result.ask_about}",
            parse_mode='md',
            buttons=buttons,
            link_preview=False,
        )
        # Сохраняем контекст в JSON с уникальным ID (для regen)
        tmp_path = os.path.join(TEMP_DIR, id_request, f"{new_id_request}.json")
        save_json({"request_to_save": request_to_save, "media_ids": media_ids, "post_text": result.post_text,
                   "url_to_msg": url_to_msg}, tmp_path)

        log("post", f"Пост отправлен, msg_id={sent.id}")

        # Удаляем pending JSON (он больше не нужен — пост ушёл на модерацию)
        pending_file = os.path.join(TEMP_DIR, id_request, f"{id_request}.json")
        if os.path.exists(pending_file):
            os.remove(pending_file)

    # --- Публикация или сохранение в pending ---
    if result.wait == False and result.post_text and not result.ask_about:
        # Пост готов: создаём уникальный ID для кнопок
        new_id_request = f"{id_request}_{uuid.uuid4().hex}"
        # Добавляем текст поста в контекст для сохранения
        request_to_save.append({
            "type": "text",
            "text": f"пост: " + result.post_text
        })

        # Кнопки модерации: опубликовать / отклонить / заново
        buttons = [
            [
                Button.inline("✅ Опубликовать", data=f"publish:{new_id_request}"),
                Button.inline("❌ Отклонить", data=f"reject:{new_id_request}"),
            ],
            [
                Button.inline("🔄 Заново", data=f"regen:{new_id_request}"),
            ],
        ]
        # Отправляем медиа отдельными сообщениями после текста
        for media_path in media_ids:
            if os.path.exists(media_path):
                await telegram_bot.send_file(ADMIN_ID, media_path)
                log("media", f"Медиа отправлено: {media_path}")

        # Отправляем пост админу на модерацию
        source_line = ""
        if url_to_msg.get("url"):
            source_line = f"[{url_to_msg.get('msg', '')}]({url_to_msg['url']})\n\n"
        sent = await telegram_bot.send_message(
            ADMIN_ID,
            source_line + result.post_text,
            parse_mode='md',
            buttons=buttons,
            link_preview=False,
        )

        # Сохраняем контекст в JSON с уникальным ID (для regen)
        tmp_path = os.path.join(TEMP_DIR, id_request, f"{new_id_request}.json")
        save_json({"request_to_save": request_to_save, "media_ids": media_ids, "post_text": result.post_text, "url_to_msg": url_to_msg}, tmp_path)

        log("post", f"Пост отправлен, msg_id={sent.id}")

        # Удаляем pending JSON (он больше не нужен — пост ушёл на модерацию)
        pending_file = os.path.join(TEMP_DIR, id_request, f"{id_request}.json")
        if os.path.exists(pending_file):
            os.remove(pending_file)

    else:
        # wait=True или пустой текст: сохраняем контекст в pending для следующего сообщения
        save_json({"request_to_save": request_to_save, "media_ids": media_ids}, pending_path)

    log("ai", f"Результат: {result}")

# Описание инструмента для Gemini (вызов словаря)
get_additional_info_tool = {
    "type": "function",
    "name": "get_additional_info",
    "description": "Получает дополнительную информацию о человеке, понятии и тд из локального словаря. Используй когда встречаешь незнакомые имена, ники, термины или аббревиатуры из контекста поста.",
    "parameters": {
        "type": "object",
        "properties": {
            "word": {
                "type": "string",
                "description": "Слово, имя, ник или понятие о котором надо получить информацию. Поддерживает не точный поиск и словосочетания.",
            },
        },
        "required": ["word"],
    },
}

def get_additional_info(word):
    """Локальный словарь: поиск информации о людях/понятиях по нечёткому совпадению"""
    dictionary = load_json("dictionary.json")
    word_lower = word.lower()
    results = []
    all_words = []
    for entry in dictionary:
        for w in entry["words"]:
            all_words.append((w, entry))
    for w, entry in all_words:
        w_lower = w.lower()
        if w_lower in word_lower or word_lower in w_lower:
            if entry not in results:
                results.append(entry)
            continue
        if any(kw.lower() in word_lower for kw in w_lower.split()):
            if entry not in results:
                results.append(entry)
            continue
        score = fuzz.ratio(word_lower, w_lower)
        if score >= 60:
            if entry not in results:
                results.append(entry)
            continue
        partial = fuzz.partial_ratio(word_lower, w_lower)
        if partial >= 75:
            if entry not in results:
                results.append(entry)
            continue
        tokens = fuzz.token_sort_ratio(word_lower, w_lower)
        if tokens >= 70:
            if entry not in results:
                results.append(entry)
    if results:
        return {"found": True, "results": results}
    return {"found": False, "query": word}

# Маппинг имён инструментов → функций для вызова Gemini
available_functions = {
    "get_additional_info": get_additional_info,
}


async def _get_entity_with_retry(client, entity, max_retries=3):
    """get_entity с повтором при RpcCallFailError / ServerError."""
    for attempt in range(1, max_retries + 1):
        try:
            return await client.get_entity(entity)
        except (RpcCallFailError, ConnectionError, OSError) as e:
            log("retry", f"get_entity({entity}) ошибка: {e} (попытка {attempt}/{max_retries})")
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
    raise

async def update_channels_mes():
    """Обновляет сообщение со списком каналов-источников в CHANNEL_ID"""
    config = load_json("config.json")
    log("file", f"Прочитан config.json:\n{json.dumps(config, ensure_ascii=False, indent=2)}")
    mes_id = config.get("channels_message_id")

    if mes_id:
        try:
            channels = ""
            ids = config.get("ids", [])
            log("file", f"IDs из config.json: {ids}")
            for i in ids:
                ch = await _get_entity_with_retry(telegram_user, i)

                name = getattr(ch, 'title', None) or ' '.join(filter(None, [getattr(ch, 'first_name', ''), getattr(ch, 'last_name', '')])) or 'Без имени'
                username = getattr(ch, "username", None)
                if not username and getattr(ch, "usernames", None):
                    username = getattr(ch.usernames[0], "username", None)

                if username:
                    channels += f"""• <a href='t.me/{username}'>{name}</a>\n"""
                else:
                    channels += f"• {name} (закрытый тгк)\n"

            await telegram_bot.edit_message(
                CHANNEL_ID,
                mes_id,
                text=f"""Информация берется с ТГК👇

<blockquote expandable>📰 Перечень каналов:
{channels}
</blockquote>
""",
                parse_mode='HTML',
                link_preview=False
            )
        except Exception as e:
            log("error", f"update_channels_mes: {e}")

@telegram_bot.on(events.NewMessage)
async def handler(event):
    """Обработчик сообщений от админа: команды и ввод для regen"""
    if event.sender_id != ADMIN_ID:
        return

    if event.raw_text == '/start':
        pass

    if event.raw_text == '/set_ch_mes':
        await event.respond('Отправь ссылку на сообщение (https://t.me/channel/123):', link_preview=False)
        bot_states[event.sender_id] = 'wait_link'
        return
    if bot_states.get(event.sender_id) == 'wait_link':
        bot_states.pop(event.sender_id, None)
        match = event.raw_text.split("/")[-1].strip()
        if match:
            msg_id = int(match)
            data = load_json("config.json")
            log("file", f"Прочитан config.json (handler):\n{json.dumps(data, ensure_ascii=False, indent=2)}")
            data["channels_message_id"] = msg_id
            save_json(data, "config.json")
            await event.respond(f"✅ ID сообщения сохранён: {msg_id}", link_preview=False)

            await update_channels_mes()
        else:
            await event.respond("❌ Неверная ссылка. Формат: https://t.me/channel/123", link_preview=False)
        return

    if event.raw_text == '/update':
        await update_channels_mes()
        await event.respond("✅ Обновили!", link_preview=False)
        return

    if event.raw_text == '/add':
        await event.respond('Отправь id канала для добавления:', link_preview=False)
        bot_states[event.sender_id] = 'wait_id_to_add'
        return
    if bot_states.get(event.sender_id) == 'wait_id_to_add':
        bot_states.pop(event.sender_id, None)

        data = load_json("config.json")
        if event.raw_text.isdigit():
            new_id = int(event.raw_text)
            ids = data.get("ids", [])
            if new_id in ids:
                await event.respond("⚠️ Этот id уже добавлен", link_preview=False)
                return
            ids.append(new_id)
            data["ids"] = ids
            save_json(data, "config.json")
            await event.respond(f"✅ ID {new_id} добавлен", link_preview=False)
        else:
            await event.respond("❌ Неверный формат, в id должны быть только цифры", link_preview=False)

        await update_channels_mes()
        return

    if event.raw_text == '/del':
        ids = load_json("config.json").get("ids", [])
        if not ids:
            await event.respond("⚠️ Список каналов пуст", link_preview=False)
            return
        channels_list = ""

        for i in ids:
            ch = await _get_entity_with_retry(telegram_user, i)

            name = getattr(ch, 'title', None) or ' '.join(
                filter(None, [getattr(ch, 'first_name', ''), getattr(ch, 'last_name', '')])) or 'Без имени'
            username = getattr(ch, "username", None)
            if not username and getattr(ch, "usernames", None):
                username = getattr(ch.usernames[0], "username", None)

            if username:
                channels_list += f"""• <a href='t.me/{username}'>{name}</a> <code>{ch.id}</code>\n"""
            else:
                channels_list += f"• {name} (закрытый тгк) <code>{ch.id}</code>\n"

        await event.respond(f"Отправь id канала для удаления:\n\n{channels_list}", link_preview=False, parse_mode='HTML')
        bot_states[event.sender_id] = 'wait_id_to_del'
        return
    if bot_states.get(event.sender_id) == 'wait_id_to_del':
        bot_states.pop(event.sender_id, None)

        data = load_json("config.json")
        if event.raw_text.isdigit():
            del_id = int(event.raw_text)
            ids = data.get("ids", [])
            if del_id not in ids:
                await event.respond("⚠️ Такого id нет в списке", link_preview=False)
                return
            ids.remove(del_id)
            data["ids"] = ids
            save_json(data, "config.json")
            await event.respond(f"✅ ID {del_id} удалён", link_preview=False)
        else:
            await event.respond("❌ Неверный формат, в id должны быть только цифры", link_preview=False)

        await update_channels_mes()
        return

    if event.raw_text == '/dict':
        dictionary = load_json("dictionary.json")
        if not dictionary:
            await event.respond("📖 Словарь пуст", link_preview=False)
            return
        lines = []
        for i, entry in enumerate(dictionary, 1):
            words = ", ".join(entry.get("words", []))
            meaning = entry.get("meaning", entry.get("text", ""))
            addition = entry.get("addition", entry.get("extra", ""))
            line = f"{i}. {words} — {meaning}"
            if addition:
                line += f" ~ {addition}"
            lines.append(line)
        text = "📖 **Словарь:**\n\n" + "\n".join(lines)
        await event.respond(text, parse_mode='md', link_preview=False)
        return

    if event.raw_text == '/dict_add':
        await event.respond("Введи: слово,слово - объяснение. ~доп инф", link_preview=False)
        bot_states[event.sender_id] = 'wait_dict_add'
        return
    if bot_states.get(event.sender_id) == 'wait_dict_add':
        bot_states.pop(event.sender_id, None)
        text = event.raw_text.strip()
        words_part, _, rest = text.partition("-")
        explanation, _, extra = rest.partition("~")
        words = [w.strip() for w in words_part.split(",") if w.strip()]
        if not words or not explanation:
            await event.respond("❌ Неверный формат. Нужно: слово,слово - объяснение. ~доп инф", link_preview=False)
            return
        entry = {
            "words": words,
            "meaning": explanation.strip(),
        }
        if extra.strip():
            entry["addition"] = [extra.strip()]
        dictionary = load_json("dictionary.json")
        dictionary.append(entry)
        save_json(dictionary, "dictionary.json")
        await event.respond("✅ Добавлено в словарь", link_preview=False)
        log("dict", f"dict_add: {words}")
        return

    if event.raw_text == '/dict_del':
        dictionary = load_json("dictionary.json")
        if not dictionary:
            await event.respond("📖 Словарь пуст", link_preview=False)
            return
        lines = []
        for i, entry in enumerate(dictionary, 1):
            words = ", ".join(entry.get("words", []))
            meaning = entry.get("meaning", entry.get("text", ""))
            line = f"{i}. {words} — {meaning}"
            lines.append(line)
        await event.respond("Введи номер записи для удаления:\n\n" + "\n".join(lines), link_preview=False)
        bot_states[event.sender_id] = 'wait_dict_del'
        return
    if bot_states.get(event.sender_id) == 'wait_dict_del':
        bot_states.pop(event.sender_id, None)
        if not event.raw_text.isdigit():
            await event.respond("❌ Введи номер цифрой", link_preview=False)
            return
        idx = int(event.raw_text) - 1
        dictionary = load_json("dictionary.json")
        if idx < 0 or idx >= len(dictionary):
            await event.respond("❌ Неверный номер", link_preview=False)
            return
        removed = dictionary.pop(idx)
        save_json(dictionary, "dictionary.json")
        words = ", ".join(removed.get("words", []))
        await event.respond(f"✅ Удалено: {words}", link_preview=False)
        log("dict", f"dict_del: {words}")
        return

    if event.raw_text == '/dict_edit':
        dictionary = load_json("dictionary.json")
        if not dictionary:
            await event.respond("📖 Словарь пуст", link_preview=False)
            return
        lines = []
        for i, entry in enumerate(dictionary, 1):
            words = ", ".join(entry.get("words", []))
            meaning = entry.get("meaning", entry.get("text", ""))
            line = f"{i}. {words} — {meaning}"
            lines.append(line)
        await event.respond("Введи номер записи для редактирования:\n\n" + "\n".join(lines), link_preview=False)
        bot_states[event.sender_id] = 'wait_dict_edit_num'
        return
    if bot_states.get(event.sender_id) == 'wait_dict_edit_num':
        if not event.raw_text.isdigit():
            await event.respond("❌ Введи номер цифрой", link_preview=False)
            return
        idx = int(event.raw_text) - 1
        dictionary = load_json("dictionary.json")
        if idx < 0 or idx >= len(dictionary):
            await event.respond("❌ Неверный номер", link_preview=False)
            return
        bot_states[event.sender_id] = f'wait_dict_edit_val:{idx}'
        entry = dictionary[idx]
        words = ", ".join(entry.get("words", []))
        meaning = entry.get("meaning", entry.get("text", ""))
        addition = entry.get("addition", entry.get("extra", ""))
        current = f"{words} — {meaning}"
        if addition:
            current += f" ~ {addition}"
        await event.respond(f"Текущее:\n{current}\n\nВведи новое: слово,слово - объяснение. ~доп инф", link_preview=False)
        return
    state = bot_states.get(event.sender_id, "")
    if state.startswith("wait_dict_edit_val:"):
        bot_states.pop(event.sender_id, None)
        idx = int(state.split(":", 1)[1])
        text = event.raw_text.strip()
        words_part, _, rest = text.partition("-")
        explanation, _, extra = rest.partition("~")
        words = [w.strip() for w in words_part.split(",") if w.strip()]
        if not words or not explanation:
            await event.respond("❌ Неверный формат. Нужно: слово,слово - объяснение. ~доп инф", link_preview=False)
            return
        dictionary = load_json("dictionary.json")
        entry = {
            "words": words,
            "meaning": explanation.strip(),
        }
        if extra.strip():
            entry["addition"] = [extra.strip()]
        dictionary[idx] = entry
        save_json(dictionary, "dictionary.json")
        await event.respond("✅ Запись обновлена", link_preview=False)
        log("dict", f"dict_edit #{idx + 1}: {words}")
        return

    if event.raw_text == '/list':
        data = load_json("config.json")
        ids = data.get("ids", [])
        if not ids:
            await event.respond("⚠ Список каналов пуст", link_preview=False)
            return

        channels = ""
        for i in ids:
            ch = await _get_entity_with_retry(telegram_user, i)

            name = getattr(ch, 'title', None) or ' '.join(
                filter(None, [getattr(ch, 'first_name', ''), getattr(ch, 'last_name', '')])) or 'Без имени'
            username = getattr(ch, "username", None)
            if not username and getattr(ch, "usernames", None):
                username = getattr(ch.usernames[0], "username", None)

            if username:
                channels += f"""• <a href='t.me/{username}'>{name}</a> <code>{ch.id}</code>\n"""
            else:
                channels += f"• {name} (закрытый тгк) <code>{ch.id}</code>\n"

        await event.respond(
            f"""
📰 Перечень каналов:
{channels}
        """,
            parse_mode='HTML',
            link_preview=False
        )

    if event.raw_text == '/gen_post':
        await event.respond("Пришлите текст и медиа", link_preview=False)
        bot_states[event.sender_id] = 'wait_text_media_for_post'
        return


    if event.raw_text == '/update_instruction':
        pass

    state = bot_states.get(event.sender_id, "")
    if state.startswith("answer:"):
        bot_states.pop(event.sender_id, None)
        request_id = state.split(":", 1)[1]
        await _answer_from_json(event, request_id)
        return

    if state.startswith("regen:"):
        bot_states.pop(event.sender_id, None)
        request_id = state.split(":", 1)[1]
        await _regen_from_json(event, request_id)
        return

    if state == "wait_text_media_for_post":
        bot_states.pop(event.sender_id, None)

        chat = await event.get_chat()
        grouped_id = event.message.grouped_id
        if grouped_id:
            if grouped_id not in album_buffers:
                album_buffers[grouped_id] = {"events": []}
            album_buffers[grouped_id]["events"].append(event)
            asyncio.create_task(delayed_album(grouped_id))
        else:
            await prepare_a_news_item(chat.id, event)

        return

    if event.raw_text == '/get':
        config = load_json("config.json")
        log("file", f"Прочитан config.json (/get):\n{json.dumps(config, ensure_ascii=False, indent=2)}")
        msg_id = config.get("channels_message_id")
        messages = await telegram_bot.get_messages(CHANNEL_ID, ids=msg_id)
        log("debug", messages.stringify() if messages else "нет сообщения")

async def _get_chat_info(event):
    """Извлекает из event имя чата, текст сообщения, прямую ссылку и id. Возвращает (name, id)"""
    chat = await event.get_chat()
    name = getattr(chat, 'title', None) or ' '.join(
        filter(None, [getattr(chat, 'first_name', ''), getattr(chat, 'last_name', '')])) or 'Без имени'
    msg_text = event.message.message if event.message.message else "(медиа)"
    username = getattr(chat, "username", None)
    if not username and getattr(chat, "usernames", None):
        username = getattr(chat.usernames[0], "username", None)
    link = f"https://t.me/{username}/{event.message.id}" if username else "нет ссылки"
    log("info", f"[{name}] {msg_text} {link}")
    return name, msg_text, link, chat.id

@telegram_user.on(events.NewMessage)
async def user_handler(event):
    """Обработчик входящих сообщений из каналов-источников"""
    if event.sender_id == ADMIN_ID:
        return

    try:
        chat = await event.get_chat()

        ids = load_json("config.json").get("ids", [])
        if (chat.id != (await telegram_bot.get_me()).id and #проверка что это не кс42
            chat.id in ids):                                #проверка что id канала в разрешенных

            _name, _msg_text, _link, _chat_id = await _get_chat_info(event)
            msg_preview = _msg_text[:25]
            await telegram_bot.send_message(ADMIN_ID, f"📩 [{_name}]({_link}): {msg_preview}", link_preview=False,
                                            parse_mode='md')

            grouped_id = event.message.grouped_id
            if grouped_id:
                if grouped_id not in album_buffers:
                    album_buffers[grouped_id] = {"events": []}
                album_buffers[grouped_id]["events"].append(event)
                asyncio.create_task(delayed_album(grouped_id))
            else:
                await prepare_a_news_item(chat.id, event)

    except Exception as e:
        log("error", f"user_handler: {e}")

async def delayed_album(grouped_id):
    """Обработка альбома: ждём ALBUM_DELAY, затем берём все фото из буфера"""
    await asyncio.sleep(ALBUM_DELAY)
    data = album_buffers.pop(grouped_id, None)
    if data:
        chat = await data["events"][0].get_chat()
        await prepare_a_news_item(chat.id, data["events"])

async def _answer_from_json(event, request_id):
    """Обрабатывает ответ админа на ask_about: сохраняет в словарь и перегенерриует пост"""
    id_request = request_id.rsplit("_", 1)[0]
    json_path = os.path.join(TEMP_DIR, id_request, f"{request_id}.json")

    if not os.path.exists(json_path):
        await telegram_bot.send_message(ADMIN_ID, "❌ Файл не найден", link_preview=False)
        return

    data = load_json(json_path)
    prepare_request = data.get("request_to_save", [])
    media_ids = data.get("media_ids", [])
    url_to_msg = data.get("url_to_msg", {"msg": "", "url": ""})

    # Парсим ответ админа: слово,слово - объяснение. ~доп инф
    text = event.raw_text.strip()
    words_part, _, rest = text.partition(" - ")
    explanation, _, extra = rest.partition(" ~ ")
    words = [w.strip() for w in words_part.split(",") if w.strip()]

    if not words or not explanation:
        await telegram_bot.send_message(ADMIN_ID, "❌ Неверный формат. Нужно: слово,слово - объяснение. ~доп инф", link_preview=False)
        return

    # Сохраняем в словарь
    entry = {
        "words": words,
        "meaning": explanation.strip(),
    }
    if extra.strip():
        entry["addition"] = extra.strip()

    dictionary = load_json("dictionary.json")
    dictionary.append(entry)
    save_json(dictionary, "dictionary.json")
    log("dict", f"Добавлено в словарь: {words}")

    # Добавляем новое знание в контекст для AI
    prepare_request.append({
        "type": "text",
        "text": f"[новое знание из словаря]: {json.dumps(entry, ensure_ascii=False)}"
    })

    # Отправляем в Gemini с обновлённым контекстом
    request_to_save = prepare_request
    to_ai = prepare_request

    previous_id = None
    while True:
        interaction = await _call_gemini(to_ai, previous_id)
        function_results = []
        for step in interaction.steps:
            if step.type == "function_call":
                result = available_functions[step.name](**step.arguments)
                log("tool", f"{step.name}({step.arguments}) → {result}")
                function_results.append({
                    "type": "function_result",
                    "name": step.name,
                    "call_id": step.id,
                    "result": [{"type": "text", "text": json.dumps(result)}],
                })
                request_to_save.append({
                    "type": "text",
                    "text": f"[инструмент {step.name}]: запрос \"{step.arguments}\" → результат: {json.dumps(result, ensure_ascii=False)}"
                })
        if not function_results:
            break
        to_ai = function_results
        previous_id = interaction.id

    result = Output.model_validate_json(interaction.output_text)

    if result.ask_about:
        await telegram_bot.send_message(ADMIN_ID, f"❌ Я всё ещё не знаю. Вопрос: {result.ask_about}", link_preview=False)
        return

    if result.post_text:
        new_id = f"{id_request}_{uuid.uuid4().hex}"
        request_to_save.append({
            "type": "text",
            "text": f"пост: " + result.post_text
        })

        buttons = [
            [
                Button.inline("✅ Опубликовать", data=f"publish:{new_id}"),
                Button.inline("❌ Отклонить", data=f"reject:{new_id}"),
            ],
            [
                Button.inline("🔄 Заново", data=f"regen:{new_id}"),
            ],
        ]
        for media_path in media_ids:
            if os.path.exists(media_path):
                await telegram_bot.send_file(ADMIN_ID, media_path)
                log("media", f"Медиа отправлено: {media_path}")

        source_line = ""
        if url_to_msg.get("url"):
            source_line = f"[{url_to_msg.get('msg', '')}]({url_to_msg['url']})\n\n"
        sent = await telegram_bot.send_message(
            ADMIN_ID,
            source_line + result.post_text,
            parse_mode='md',
            buttons=buttons,
            link_preview=False,
        )

        tmp_path = os.path.join(TEMP_DIR, id_request, f"{new_id}.json")
        save_json({"request_to_save": request_to_save, "media_ids": media_ids, "post_text": result.post_text, "url_to_msg": url_to_msg}, tmp_path)

        log("answer", f"Пост с новым знанием отправлен, msg_id={sent.id}")

        if os.path.exists(json_path):
            os.remove(json_path)
    else:
        await telegram_bot.send_message(ADMIN_ID, "❌ Нет текста поста после добавления в словарь", link_preview=False)


async def _regen_from_json(event, request_id):
    """Регенерация поста: загружает JSON, добавляет фидбэк админа, перегенерирует"""
    # Извлекаем chat_id из request_id (формат: chat_id_uuid)
    id_request = request_id.rsplit("_", 1)[0]
    # Путь к JSON с контекстом этого поста
    json_path = os.path.join(TEMP_DIR, id_request, f"{request_id}.json")

    # Проверяем что JSON существует
    if not os.path.exists(json_path):
        await telegram_bot.send_message(ADMIN_ID, "❌ Файл не найден", link_preview=False)
        return

    # Загружаем контекст (инструкция + канал + сообщения + инструменты + предыдущий пост)
    data = load_json(json_path)
    prepare_request = data.get("request_to_save", [])
    media_ids = data.get("media_ids", [])
    url_to_msg = data.get("url_to_msg", {"msg": "", "url": ""})

    log("file", f"Прочитан {json_path}:\n{json.dumps(prepare_request, ensure_ascii=False, indent=2)}")
    if not prepare_request:
        await telegram_bot.send_message(ADMIN_ID, "❌ Файл пуст", link_preview=False)
        return

    # Добавляем фидбэк админа в контекст
    feedback = event.raw_text.strip()
    prepare_request.append({
        "type": "text",
        "text": "обратная связь от админа: " + feedback,
    })
    log("regen", f"request_id={request_id}, feedback={feedback}...")

    # Отправляем обновлённый контекст в Gemini
    request_to_save = prepare_request
    to_ai = prepare_request

    previous_id = None  # ID предыдущего взаимодействия для цепочки
    while True:
        interaction = await _call_gemini(to_ai, previous_id)
        # Обрабатываем вызовы инструментов
        function_results = []
        for step in interaction.steps:
            if step.type == "function_call":
                result = available_functions[step.name](**step.arguments)
                log("tool", f"{step.name}({step.arguments}) → {result}")
                function_results.append({
                    "type": "function_result",
                    "name": step.name,
                    "call_id": step.id,
                    "result": [{"type": "text", "text": json.dumps(result)}],
                })
                # Сохраняем вызов инструмента в контекст
                request_to_save.append({
                    "type": "text",
                    "text": f"[инструмент {step.name}]: запрос \"{step.arguments}\" → результат: {json.dumps(result, ensure_ascii=False)}"
                })
        # Если не было вызовов — AI готовит ответ
        if not function_results:
            break
        to_ai = function_results
        previous_id = interaction.id

    # Парсим ответ Gemini
    result = Output.model_validate_json(interaction.output_text)

    # Если есть текст поста — отправляем с кнопками модерации
    if result.post_text:
        new_id = f"{id_request}_{uuid.uuid4().hex}"
        # Добавляем новый пост в контекст (для возможного следующего regen)
        request_to_save.append({
            "type": "text",
            "text": f"пост: " + result.post_text
        })

        # Кнопки модерации
        buttons = [
            [
                Button.inline("✅ Опубликовать", data=f"publish:{new_id}"),
                Button.inline("❌ Отклонить", data=f"reject:{new_id}"),
            ],
            [
                Button.inline("🔄 Заново", data=f"regen:{new_id}"),
            ],
        ]
        # Отправляем медиа отдельными сообщениями после текста
        for media_path in media_ids:
            if os.path.exists(media_path):
                await telegram_bot.send_file(ADMIN_ID, media_path)
                log("media", f"Медиа отправлено: {media_path}")
        # Отправляем новый вариант поста админу
        source_line = ""
        if url_to_msg.get("url"):
            source_line = f"[{url_to_msg.get('msg', '')}]({url_to_msg['url']})\n\n"
        sent = await telegram_bot.send_message(
            ADMIN_ID,
            source_line + result.post_text,
            parse_mode='md',
            buttons=buttons,
            link_preview=False,
        )

        # Сохраняем контекст с новым ID
        tmp_path = os.path.join(TEMP_DIR, id_request, f"{new_id}.json")
        save_json({"request_to_save": request_to_save, "media_ids": media_ids, "post_text": result.post_text, "url_to_msg": url_to_msg}, tmp_path)

        log("regen", f"Новый пост отправлен, msg_id={sent.id}")

        # Удаляем старый JSON (он заменён новым)
        if os.path.exists(json_path):
            os.remove(json_path)
    else:
        # Нет текста поста — сообщаем админу об ошибке
        await telegram_bot.send_message(ADMIN_ID, "❌ Нет текста поста", link_preview=False)
        log("regen", "result.post_text пуст")

@telegram_bot.on(events.CallbackQuery)
async def callback_handler(event):
    """Обработчик кнопок модерации: publish/reject/regen"""
    data = event.data.decode("utf-8")
    action, request_id = data.split(":", 1)
    log("callback", f"action={action} request_id={request_id}")

    if action == "publish":
        # Извлекаем chat_id из request_id (формат: chat_id_uuid)
        id_request = request_id.rsplit("_", 1)[0]
        json_path = os.path.join(TEMP_DIR, id_request, f"{request_id}.json")
        if not os.path.exists(json_path):
            await event.answer("❌ JSON не найден", alert=True)
            return

        log("var", f"id_request={id_request}")
        log("var", f"json_path={json_path}")

        data_json = load_json(json_path)
        post_text = data_json.get("post_text", "")
        media_ids = data_json.get("media_ids", [])
        url_to_msg = data_json.get("url_to_msg", {})

        source_line = ""
        if url_to_msg.get("url"):
            source_line = f"[{url_to_msg.get('msg', '')}]({url_to_msg['url']})\n\n"
        full_text = source_line + post_text

        existing_media = [m for m in media_ids if os.path.exists(m)]
        if existing_media:
            await telegram_user.send_file(CHANNEL_ID, existing_media, caption=full_text, parse_mode='md')
        else:
            await telegram_user.send_message(CHANNEL_ID, full_text, parse_mode='md', link_preview=False)

        log("publish", f"Опубликовано в CHANNEL_ID: {request_id}")

        # Удаляем JSON и медиа-файлы
        os.remove(json_path)
        for m in existing_media:
            os.remove(m)

        msg = await event.get_message()
        await event.edit(text=msg.text + "\n\n__✅ Опубликовано__", buttons=None, link_preview=False, parse_mode='md')
    elif action == "reject":
        # Извлекаем chat_id из request_id (формат: chat_id_uuid)
        id_request = request_id.rsplit("_", 1)[0]
        json_path = os.path.join(TEMP_DIR, id_request, f"{request_id}.json")
        if os.path.exists(json_path):
            data_json = load_json(json_path)
            for m in data_json.get("media_ids", []):
                if os.path.exists(m):
                    os.remove(m)
            os.remove(json_path)

        msg = await event.get_message()
        await event.edit(text=msg.text + "\n\n__❌ Отклонено__", buttons=None, link_preview=False, parse_mode='md')
    elif action == "regen":
        msg = await event.get_message()
        await event.edit(
            text=msg.text + "\n\n__🔄 Отправлено на перегенерацию...__",
            buttons=None,
            link_preview=False,
            parse_mode='md',
        )
        bot_states[ADMIN_ID] = f"regen:{request_id}"
        await telegram_bot.send_message(ADMIN_ID, "Что не так? Что нужно изменить?", link_preview=False)
    elif action == "answer":
        msg = await event.get_message()
        await event.edit(
            text=msg.text + "\n\n__Отправлено на дополнение словаря...__",
            buttons=None,
            link_preview=False,
            parse_mode='md',
        )
        bot_states[ADMIN_ID] = f"answer:{request_id}"
        await telegram_bot.send_message(ADMIN_ID, "формат: слово,слово - объяснение. ~доп инф. ссылки и тд", link_preview=False)
    elif action == "cancel":
        id_request = request_id.rsplit("_", 1)[0]
        json_path = os.path.join(TEMP_DIR, id_request, f"{request_id}.json")
        if os.path.exists(json_path):
            data_json = load_json(json_path)
            for m in data_json.get("media_ids", []):
                if os.path.exists(m):
                    os.remove(m)
            os.remove(json_path)

        msg = await event.get_message()
        await event.edit(text=msg.text + "\n\n__❌ Отменено__", buttons=None, link_preview=False, parse_mode='md')

async def main():
    """Запуск: авторизация user → старт бота → обработка сообщений"""
    await telegram_user.connect()
    if not await telegram_user.is_user_authorized():
        phone = input("Phone: ")
        await telegram_user.send_code_request(phone)
        code = input("Code: ")
        try:
            await telegram_user.sign_in(phone, code)
        except SessionPasswordNeededError:
            password = input("2FA password: ")
            await telegram_user.sign_in(password=password)
    me = await telegram_user.get_me()
    log("auth", f"Пользовательский аккаунт подключён! ID: {me.id}")

    await telegram_bot.start(bot_token=os.getenv("telegram_bot_api"))
    log("auth", "Бот запущен!")

    await update_channels_mes()

    # Бесконечный перезапуск при RpcCallFailError (Telegram internal issues)
    while True:
        try:
            await asyncio.gather(
                telegram_bot.run_until_disconnected(),
                telegram_user.run_until_disconnected(),
            )
        except (RpcCallFailError, ConnectionError, OSError) as e:
            log("error", f"Сбой соединения: {e}, перезапуск через 5 с...")
            await asyncio.sleep(5)
            await telegram_user.connect()
            if not await telegram_user.is_user_authorized():
                raise
            await telegram_bot.start(bot_token=os.getenv("telegram_bot_api"))

if __name__ == '__main__':
    asyncio.run(main())