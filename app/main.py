import os
import asyncio
import json
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import aiohttp
import asyncpg
from sql_templates import build_query
from pathlib import Path
import base64
import uuid
import time
import ssl

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_DSN = os.getenv("DATABASE_DSN", "postgresql://postgres:postgres@db:5432/postgres")
GIGACHAT_URL = os.getenv("GIGACHAT_API_URL")
GIGACHAT_KEY = os.getenv("GIGACHAT_API_KEY")
PROMPT_PATH = os.getenv("PROMPT_PATH", "/app/app/nl2json_prompt.txt")
GIGACHAT_CLIENT_ID = os.getenv("GIGACHAT_CLIENT_ID")
GIGACHAT_CLIENT_SECRET = os.getenv("GIGACHAT_CLIENT_SECRET")
GIGACHAT_AUTH_URL = os.getenv("GIGACHAT_AUTH_URL")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

_gigachat_token = None
_token_expires_at = 0

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async def call_gigachat(user_text: str):
    prompt = Path(PROMPT_PATH).read_text(encoding="utf-8")
    token = await get_gigachat_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "GigaChat",
        "messages": [{
                "role": "system",
                "content": prompt
            },
            {
                "role": "user",
                "content": user_text
            }
        ],
        "temperature": 0.1
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(GIGACHAT_URL, json=payload, headers=headers, timeout=30, ssl=ssl_context) as resp:
            data = await resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        raise Exception(f"Gigachat response error: {data}")
    return content

async def execute_parsed_and_respond(chat_id, parsed_json, conn):
    sql, params = build_query(parsed_json)
    if not sql:
        await bot.send_message(chat_id, "Не могу сформировать запрос по этому тексту.")
        return
    try:
        row = await conn.fetchrow(sql, *params)
        val = None
        if row:
            vals = list(row)
            if vals:
                val = vals[0]
        if val is None:
            val = 0
        await bot.send_message(chat_id, str(int(val)))
    except Exception as e:
        logging.exception("DB error")
        await bot.send_message(chat_id, f"Ошибка при выполнении запроса: {e}")

@dp.message(Command(commands=["start"]))
async def start_cmd(message: types.Message):
    await message.reply("Привет! Отправь мне вопрос на русском про статистику видео — я верну одно число.")

@dp.message()
async def handle_text(message: types.Message):
    user_text = message.text
    chat_id = message.chat.id
    await bot.send_chat_action(chat_id, "typing")

    try:
        resp_text = await call_gigachat(user_text)
    except Exception as e:
        logging.exception("LLM call failed")
        await bot.send_message(chat_id, "Ошибка при обращении к нейросети.")
        return

    try:
        parsed = json.loads(resp_text)
    except Exception:
        import re
        m = re.search(r'(\{.*\})', resp_text, flags=re.S)
        if m:
            parsed = json.loads(m.group(1))
        else:
            await bot.send_message(chat_id, "Невозможно распознать ответ нейросети.")
            return

    conn = await asyncpg.connect(DATABASE_DSN)
    try:
        await execute_parsed_and_respond(chat_id, parsed, conn)
    finally:
        await conn.close()

async def get_gigachat_token():
    global _gigachat_token, _token_expires_at

    if _gigachat_token and time.time() < _token_expires_at - 60:
        return _gigachat_token

    client_id = GIGACHAT_CLIENT_ID.strip()
    client_secret = GIGACHAT_CLIENT_SECRET.strip()

    auth_string = f"{client_id}:{client_secret}"
    auth_base64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_base64}",
        "RqUID": str(uuid.uuid4()),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    data = {
        "scope": "GIGACHAT_API_PERS"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(GIGACHAT_AUTH_URL, headers=headers, data=data, ssl=ssl_context, timeout=30) as resp:
            text = await resp.text()
            try:
                result = await resp.json()
            except:
                raise Exception(f"Gigachat auth raw response: {text}")

    if "access_token" not in result:
        raise Exception(f"Gigachat auth error: {result}")

    _gigachat_token = result["access_token"]
    _token_expires_at = time.time() + 1700

    print("GigaChat token received")

    return _gigachat_token

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())