from telethon import TelegramClient, events
import os
import asyncio
from defunc import getoptions

MESSAGE_FILE = 'message.txt'

options = getoptions()
api_id = int(options[0].strip())
api_hash = options[1].strip()
bot_token = options[4].strip()

bot = TelegramClient('manager_bot', api_id, api_hash).start(bot_token=bot_token)

async def get_sessions():
    return [f for f in os.listdir('.') if f.endswith('.session')]

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        '/stats - статистика\n'
        '/set_message <текст> - текст рассылки\n'
        '/add_user <username> - добавить пользователя\n'
        '/clear_users - очистить список\n'
        '/test <username> - тестовая отправка\n'
        '/send - запустить рассылку'
    )

@bot.on(events.NewMessage(pattern='/stats'))
async def stats(event):
    sessions = await get_sessions()
    user_count = 0
    if os.path.exists('usernames.txt'):
        with open('usernames.txt') as f:
            user_count = len([u for u in f if u.strip()])
    await event.respond(f'Аккаунтов: {len(sessions)}\nПользователей: {user_count}')

@bot.on(events.NewMessage(pattern='/set_message'))
async def set_message(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /set_message текст')
        return
    with open(MESSAGE_FILE, 'w') as f:
        f.write(parts[1])
    await event.respond('Текст сохранён')


@bot.on(events.NewMessage(pattern='/add_user'))
async def add_user(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /add_user username')
        return
    with open('usernames.txt', 'a') as f:
        f.write(parts[1].strip() + '\n')
    await event.respond('Пользователь добавлен')


@bot.on(events.NewMessage(pattern='/clear_users'))
async def clear_users(event):
    open('usernames.txt', 'w').close()
    await event.respond('Список пользователей очищен')

@bot.on(events.NewMessage(pattern='/test'))
async def test(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /test username')
        return
    if not os.path.exists(MESSAGE_FILE):
        await event.respond('Сначала задайте текст через /set_message')
        return
    sessions = await get_sessions()
    if not sessions:
        await event.respond('Нет аккаунтов')
        return
    with open(MESSAGE_FILE) as f:
        msg = f.read()
    client = TelegramClient(sessions[0], api_id, api_hash)
    await client.start()
    await client.send_message(parts[1], msg)
    await client.disconnect()
    await event.respond('Отправлено')

@bot.on(events.NewMessage(pattern='/send'))
async def send_all(event):
    if not os.path.exists(MESSAGE_FILE):
        await event.respond('Сначала задайте текст через /set_message')
        return
    if not os.path.exists('usernames.txt'):
        await event.respond('Нет файла usernames.txt')
        return
    with open('usernames.txt') as f:
        users = [u.strip() for u in f if u.strip()]
    if not users:
        await event.respond('Нет пользователей для рассылки')
        return
    sessions = await get_sessions()
    if not sessions:
        await event.respond('Нет аккаунтов')
        return
    with open(MESSAGE_FILE) as f:
        msg = f.read()
    for session in sessions:
        client = TelegramClient(session, api_id, api_hash)
        await client.start()
        for user in users:
            try:
                await client.send_message(user, msg)
                await asyncio.sleep(1)
            except Exception:
                continue
        await client.disconnect()
    await event.respond('Рассылка завершена')

bot.run_until_disconnected()
