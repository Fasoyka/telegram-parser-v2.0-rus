from collections import deque
from functools import wraps
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
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

# Храним состояние аккаунтов: ok или текст ошибки
account_status = {}
available_chats = []


def notify_errors(func):
    @wraps(func)
    async def wrapper(event, *args, **kwargs):
        try:
            return await func(event, *args, **kwargs)
        except Exception as e:
            await event.respond(f'Ошибка: {type(e).__name__}: {e}')
    return wrapper

@bot.on(events.NewMessage(pattern='/start'))
@notify_errors
async def start(event):
    await event.respond(
        '/stats - статистика\n'
        '/set_message <текст> - текст рассылки\n'
        '/add_user <username> - добавить пользователя\n'
        '/clear_users - очистить список\n'
        '/users - файл с пользователями\n'
        '/chats - список чатов для парсинга\n'
        '/parse <номер> - спарсить чат\n'
        '/test <username> - тестовая отправка\n'
        '/send - запустить рассылку\n'
        '/end - лог рассылки'
    )

@bot.on(events.NewMessage(pattern='/stats'))
@notify_errors
async def stats(event):
    sessions = await get_sessions()
    user_count = 0
    if os.path.exists('usernames.txt'):
        with open('usernames.txt') as f:
            user_count = len([u for u in f if u.strip()])
    lines = []
    for s in sessions:
        status = account_status.get(s, 'unknown')
        lines.append(f'{s}: {status}')
    await event.respond('Аккаунты:\n' + '\n'.join(lines) + f'\nПользователей: {user_count}')

@bot.on(events.NewMessage(pattern='/set_message'))
@notify_errors
async def set_message(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /set_message текст')
        return
    with open(MESSAGE_FILE, 'w') as f:
        f.write(parts[1])
    await event.respond('Текст сохранён')


@bot.on(events.NewMessage(pattern='/add_user'))
@notify_errors
async def add_user(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /add_user username')
        return
    with open('usernames.txt', 'a') as f:
        f.write(parts[1].strip() + '\n')
    await event.respond('Пользователь добавлен')


@bot.on(events.NewMessage(pattern='/clear_users'))
@notify_errors
async def clear_users(event):
    open('usernames.txt', 'w').close()
    await event.respond('Список пользователей очищен')


@bot.on(events.NewMessage(pattern='/users'))
@notify_errors
async def send_users_file(event):
    if os.path.exists('usernames.txt'):
        if os.path.getsize('usernames.txt') > 0:
            await event.respond(file='usernames.txt')
        else:
            await event.respond('Файл usernames.txt пуст')
    else:
        await event.respond('Файл usernames.txt не найден')


@bot.on(events.NewMessage(pattern='/chats'))
@notify_errors
async def list_chats(event):
    sessions = await get_sessions()
    if not sessions:
        await event.respond('Нет аккаунтов')
        return
    groups = []
    chat_map = []
    seen_ids = set()
    for session in sessions:
        client = TelegramClient(session, api_id, api_hash)
        try:
            await client.start()
            result = await client(
                GetDialogsRequest(
                    offset_date=None,
                    offset_id=0,
                    offset_peer=InputPeerEmpty(),
                    limit=200,
                    hash=0,
                )
            )
            for chat in result.chats:
                try:
                    if chat.megagroup and chat.id not in seen_ids:
                        seen_ids.add(chat.id)
                        groups.append(f"{chat.title} ({session})")
                        chat_map.append((session, chat))
                except AttributeError:
                    continue
        finally:
            await client.disconnect()
    if not chat_map:
        await event.respond('Нет доступных групп')
        return
    global available_chats
    available_chats = chat_map
    lines = [f'{i} - {title}' for i, title in enumerate(groups)]
    await event.respond('Доступные чаты:\n' + '\n'.join(lines) + '\nИспользуйте /parse <номер>')


@bot.on(events.NewMessage(pattern='/parse'))
@notify_errors
async def parse_command(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /parse номер')
        return
    if not available_chats:
        await event.respond('Сначала выполните /chats')
        return
    index = parts[1]
    if index == 'all':
        targets = available_chats
    else:
        try:
            targets = [available_chats[int(index)]]
        except (ValueError, IndexError):
            await event.respond('Неверный номер чата')
            return
    opts = getoptions()
    parse_ids = opts[2].strip() == 'True'
    parse_names = opts[3].strip() == 'True'
    existing_names = set()
    if parse_names and os.path.exists('usernames.txt'):
        with open('usernames.txt') as f:
            existing_names = {u.strip() for u in f if u.strip()}
        names_file = open('usernames.txt', 'a')
    else:
        names_file = None
    existing_ids = set()
    if parse_ids and os.path.exists('userids.txt'):
        with open('userids.txt') as f:
            existing_ids = {u.strip() for u in f if u.strip()}
        ids_file = open('userids.txt', 'a')
    else:
        ids_file = None
    for session, chat in targets:
        client = TelegramClient(session, api_id, api_hash)
        try:
            await client.start()
            participants = await client.get_participants(chat)
            for user in participants:
                if parse_names and user.username and 'bot' not in user.username.lower():
                    uname = '@' + user.username
                    if uname not in existing_names:
                        names_file.write(uname + '\n')
                        existing_names.add(uname)
                if parse_ids:
                    uid = str(user.id)
                    if uid not in existing_ids:
                        ids_file.write(uid + '\n')
                        existing_ids.add(uid)
        finally:
            await client.disconnect()
    if names_file:
        names_file.close()
    if ids_file:
        ids_file.close()
    await event.respond('Спаршено')

@bot.on(events.NewMessage(pattern='/test'))
@notify_errors
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
@notify_errors
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

    clients = {}
    account_status.clear()
    for session in sessions:
        client = TelegramClient(session, api_id, api_hash)
        try:
            await client.start()
            clients[session] = client
            account_status[session] = 'ok'
        except Exception as e:
            account_status[session] = f'error: {type(e).__name__}'

    if not clients:
        await event.respond('Нет рабочих аккаунтов')
        return

    queue = deque(clients.items())
    failed_users = []
    log_lines = []

    for user in users:
        delivered = False
        attempts = 0
        error_text = ''
        while queue and attempts < len(queue):
            session, client = queue[0]
            try:
                await client.send_message(user, msg)
                queue.rotate(-1)
                delivered = True
                log_lines.append(f'{user}: delivered')
                await asyncio.sleep(1)
                break
            except Exception as e:
                await client.disconnect()
                account_status[session] = f'error: {type(e).__name__}'
                queue.popleft()
                attempts += 1
                error_text = f'{type(e).__name__}: {e}'
        if not delivered:
            failed_users.append(user)
            log_lines.append(f'{user}: {error_text or "failed"}')

    for _, client in queue:
        await client.disconnect()

    with open('send_log.txt', 'w') as log_file:
        log_file.write('\n'.join(log_lines))

    if failed_users:
        await event.respond('Не доставлено: ' + ', '.join(failed_users))
    else:
        await event.respond('Рассылка завершена')

@bot.on(events.NewMessage(pattern='/end'))
@notify_errors
async def send_log(event):
    if os.path.exists('send_log.txt'):
        await event.respond(file='send_log.txt')
    else:
        await event.respond('Лог файл не найден')

# Приём текстового файла со списком пользователей
@bot.on(events.NewMessage)
@notify_errors
async def handle_user_file(event):
    if event.raw_text.startswith('/'):
        return
    if event.message.file and event.message.file.name.endswith('.txt'):
        await event.message.download_media(file='usernames.txt')
        await event.respond('Список пользователей обновлён')

bot.run_until_disconnected()
