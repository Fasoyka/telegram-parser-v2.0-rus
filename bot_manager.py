from collections import deque
from functools import wraps
from telethon import TelegramClient, events, Button
from telethon.errors import SessionPasswordNeededError, MessageTooLongError
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
import os
import asyncio
import re
from datetime import datetime
from defunc import getoptions

LISTS_DIR = 'lists'
os.makedirs(LISTS_DIR, exist_ok=True)
USER_FILE = os.path.join(LISTS_DIR, 'usernames.txt')
MESSAGE_FILE = 'message.txt'

options = getoptions()
api_id = int(options[0].strip())
api_hash = options[1].strip()
bot_token = options[4].strip()

bot = TelegramClient('manager_bot', api_id, api_hash).start(bot_token=bot_token)

async def get_sessions():
    return [
        f
        for f in os.listdir('.')
        if f.endswith('.session') and f != 'manager_bot.session'
    ]


def get_user_lists():
    return sorted(
        os.path.join(LISTS_DIR, f)
        for f in os.listdir(LISTS_DIR)
        if f.endswith('.txt')
    )

# Храним состояние аккаунтов: ok или текст ошибки
account_status = {}
available_chats = []
# Глобальная блокировка для исключения одновременного доступа к сессиям
session_lock = asyncio.Lock()


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
    keyboard = [
        [Button.text('/stats'), Button.text('/chats'), Button.text('/lists')],
        [Button.text('/clear_users'), Button.text('/end'), Button.text('/sessions')],
        [Button.text('/add_session')],
    ]
    await event.respond(
        'Выберите команду на клавиатуре ниже.\n'
        'Для команд с параметрами используйте ввод вручную:\n'
        '/set_message <текст> - текст рассылки\n'
        '/add_user <username> - добавить пользователя\n'
        '/users <номер> - отправить список\n'
        '/parse <номер> - спарсить чат\n'
        '/del_list <номер> - удалить список\n'
        '/split <номер> <частей> - разделить список\n'
        '/test <username> - тестовая отправка\n'
        '/send <номер> - запустить рассылку\n'
        '/del_session <имя> - удалить сессию',
        buttons=keyboard,
    )

@bot.on(events.NewMessage(pattern='/stats'))
@notify_errors
async def stats(event):
    sessions = await get_sessions()
    user_count = 0
    for fname in get_user_lists():
        with open(fname) as f:
            user_count += len([u for u in f if u.strip()])
    lines = []
    for s in sessions:
        status = account_status.get(s, 'unknown')
        lines.append(f'{s}: {status}')
    await event.respond('Аккаунты:\n' + '\n'.join(lines) + f'\nПользователей: {user_count}')


@bot.on(events.NewMessage(pattern='/sessions'))
@notify_errors
async def list_sessions_cmd(event):
    sessions = await get_sessions()
    if sessions:
        await event.respond('Сессии:\n' + '\n'.join(sessions))
    else:
        await event.respond('Нет сессий')


@bot.on(events.NewMessage(pattern='/del_session'))
@notify_errors
async def del_session(event):
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) < 2:
        await event.respond('Использование: /del_session имя')
        return
    name = parts[1].strip()
    if not name.endswith('.session'):
        name += '.session'
    if name == 'manager_bot.session':
        await event.respond('Нельзя удалить сессию менеджера')
        return
    async with session_lock:
        if os.path.exists(name):
            os.remove(name)
            account_status.pop(name, None)
            await event.respond('Сессия удалена')
        else:
            await event.respond('Сессия не найдена')


@bot.on(events.NewMessage(pattern='/add_session'))
@notify_errors
async def add_session(event):
    # Если пришёл файл .session, сохраняем его как раньше
    if event.message.file and event.message.file.name.endswith('.session'):
        async with session_lock:
            await event.message.download_media(file=event.message.file.name)
        await event.respond('Сессия добавлена')
        return

    # Иначе запускаем интерактивное добавление через телефон
    async with bot.conversation(event.chat_id, timeout=120) as conv:
        await conv.send_message('Введите номер телефона в формате +79990000000')
        phone = (await conv.get_response()).raw_text.strip()
        session_name = phone.replace('+', '').replace(' ', '')
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()
        try:
            await client.send_code_request(phone)
            await conv.send_message('Введите код из Telegram:')
            code = (await conv.get_response()).raw_text.strip()
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                await conv.send_message('Введите пароль:')
                password = (await conv.get_response()).raw_text.strip()
                await client.sign_in(password=password)
            account_status[f'{session_name}.session'] = 'ok'
            await conv.send_message('Сессия добавлена')
        except Exception as e:
            await conv.send_message(f'Ошибка: {e}')
        finally:
            await client.disconnect()

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
    with open(USER_FILE, 'a') as f:
        f.write(parts[1].strip() + '\n')
    await event.respond('Пользователь добавлен')


@bot.on(events.NewMessage(pattern='/clear_users'))
@notify_errors
async def clear_users(event):
    open(USER_FILE, 'w').close()
    await event.respond('Список пользователей очищен')


@bot.on(events.NewMessage(pattern='/users'))
@notify_errors
async def send_users_file(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /users номер')
        return
    files = get_user_lists()
    try:
        fname = files[int(parts[1])]
    except (ValueError, IndexError):
        await event.respond('Неверный номер списка')
        return
    if os.path.getsize(fname) > 0:
        await event.respond(file=fname)
    else:
        await event.respond('Файл пуст')


@bot.on(events.NewMessage(pattern='/chats'))
@notify_errors
async def list_chats(event):
    async with session_lock:
        sessions = await get_sessions()
        if not sessions:
            await event.respond('Нет аккаунтов')
            return
        groups = []
        chat_map = []
        seen_ids = set()
        for session in sessions:
            async with TelegramClient(session, api_id, api_hash) as client:
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
        if not chat_map:
            await event.respond('Нет доступных групп')
            return
        global available_chats
        available_chats = chat_map
        lines = [f'{i} - {title}' for i, title in enumerate(groups)]
        message = 'Доступные чаты:\n' + '\n'.join(lines) + '\nИспользуйте /parse <номер>'
        try:
            await event.respond(message)
        except MessageTooLongError:
            filename = 'chats.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(message)
            await event.respond('Список чатов слишком длинный, отправляю файлом:', file=filename)
            os.remove(filename)


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
    async with session_lock:
        opts = getoptions()
        parse_ids = opts[2].strip() == 'True'
        parse_names = opts[3].strip() == 'True'
        created_files = []
        for session, chat in targets:
            async with TelegramClient(session, api_id, api_hash) as client:
                participants = await client.get_participants(chat)
            names = []
            ids = []
            for user in participants:
                if parse_names and user.username and 'bot' not in user.username.lower():
                    names.append('@' + user.username)
                if parse_ids:
                    ids.append(str(user.id))

            prefix = re.sub(r"\W+", "_", chat.title)[:10]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if parse_names and names:
                name_file = os.path.join(LISTS_DIR, f"users_{prefix}_{timestamp}.txt")
                with open(name_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(names))
                created_files.append(os.path.basename(name_file))
            if parse_ids and ids:
                id_file = os.path.join(LISTS_DIR, f"ids_{prefix}_{timestamp}.txt")
                with open(id_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(ids))
                created_files.append(os.path.basename(id_file))
        msg = 'Спаршено'
        if created_files:
            msg += ': ' + ', '.join(created_files)
        await event.respond(msg)


@bot.on(events.NewMessage(pattern='/lists'))
@notify_errors
async def list_user_lists(event):
    files = get_user_lists()
    if not files:
        await event.respond('Нет списков')
        return
    lines = [f'{i} - {os.path.basename(name)}' for i, name in enumerate(files)]
    await event.respond('Списки:\n' + '\n'.join(lines))


@bot.on(events.NewMessage(pattern='/del_list'))
@notify_errors
async def delete_list(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /del_list номер')
        return
    files = get_user_lists()
    try:
        fname = files[int(parts[1])]
    except (ValueError, IndexError):
        await event.respond('Неверный номер списка')
        return
    os.remove(fname)
    await event.respond('Список удалён')


@bot.on(events.NewMessage(pattern='/split'))
@notify_errors
async def split_list(event):
    parts = event.raw_text.split()
    if len(parts) < 3:
        await event.respond('Использование: /split номер частей')
        return
    files = get_user_lists()
    try:
        fname = files[int(parts[1])]
    except (ValueError, IndexError):
        await event.respond('Неверный номер списка')
        return
    try:
        pieces = int(parts[2])
    except ValueError:
        await event.respond('Неверное количество частей')
        return
    if pieces not in (2, 3, 4):
        await event.respond('Допустимые варианты: 2, 3, 4')
        return
    with open(fname) as f:
        users = [u.strip() for u in f if u.strip()]
    if not users:
        await event.respond('Список пуст')
        return
    base = fname.rsplit('.txt', 1)[0]
    size = len(users) // pieces
    rem = len(users) % pieces
    start = 0
    for i in range(pieces):
        end = start + size + (1 if i < rem else 0)
        part_file = f'{base}_part{i+1}.txt'
        with open(part_file, 'w') as pf:
            pf.write('\n'.join(users[start:end]))
        start = end
    await event.respond('Список разделён')

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
    async with session_lock:
        sessions = await get_sessions()
        if not sessions:
            await event.respond('Нет аккаунтов')
            return
        with open(MESSAGE_FILE) as f:
            msg = f.read()
        async with TelegramClient(sessions[0], api_id, api_hash) as client:
            await client.send_message(parts[1], msg)
    await event.respond('Отправлено')

@bot.on(events.NewMessage(pattern='/send'))
@notify_errors
async def send_all(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /send номер')
        return
    files = get_user_lists()
    try:
        fname = files[int(parts[1])]
    except (ValueError, IndexError):
        await event.respond('Неверный номер списка')
        return
    if not os.path.exists(MESSAGE_FILE):
        await event.respond('Сначала задайте текст через /set_message')
        return
    with open(fname) as f:
        users = [u.strip() for u in f if u.strip()]
    if not users:
        await event.respond('Нет пользователей для рассылки')
        return
    async with session_lock:
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
        path = os.path.join(LISTS_DIR, event.message.file.name)
        await event.message.download_media(file=path)
        await event.respond('Список пользователей сохранён')

bot.run_until_disconnected()
