from collections import deque
from functools import wraps
from telethon import TelegramClient, events, Button
from telethon.errors import SessionPasswordNeededError, MessageTooLongError
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
import os
import asyncio
import re
import sqlite3
from datetime import datetime, timezone
import zipfile
from io import BytesIO
import socks
from defunc import getoptions, clear_user_lists, LISTS_DIR, SESSIONS_DIR

USER_FILE = os.path.join(LISTS_DIR, 'usernames.txt')
MESSAGE_FILE = 'message.txt'
MESSAGE1_FILE = 'message1.txt'
MESSAGE2_FILE = 'message2.txt'

options = getoptions()
api_id = int(options[0].strip())
api_hash = options[1].strip()
bot_token = options[4].strip()

bot = TelegramClient('manager_bot', api_id, api_hash).start(bot_token=bot_token)

PROXY_FILE = 'proxies.txt'
DELAY_FILE = 'delay.txt'


def load_delay():
    if os.path.exists(DELAY_FILE):
        try:
            with open(DELAY_FILE) as f:
                return float(f.read().strip())
        except ValueError:
            pass
    return 5.0


def save_delay(value):
    with open(DELAY_FILE, 'w') as f:
        f.write(str(value))


message_delay = load_delay()


async def get_sessions():
    sessions = sorted(
        f
        for f in os.listdir(SESSIONS_DIR)
        if f.endswith('.session')
    )
    for name in sessions:
        path = os.path.join(SESSIONS_DIR, name)
        try:
            with sqlite3.connect(path) as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(sessions)")
                cols = [row[1] for row in cur.fetchall()]
                if 'version' not in cols:
                    cur.execute(
                        "ALTER TABLE sessions ADD COLUMN version INTEGER DEFAULT 0"
                    )
                conn.commit()
        except sqlite3.Error:
            pass
    return sessions


def load_proxies():
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE) as f:
        return [line.strip() for line in f if line.strip()]


def save_proxies(proxies):
    with open(PROXY_FILE, 'w') as f:
        if proxies:
            f.write('\n'.join(proxies) + '\n')


def get_user_lists():
    return sorted(
        os.path.join(LISTS_DIR, f)
        for f in os.listdir(LISTS_DIR)
        if f.endswith('.txt')
    )


async def reply_watcher(client, usernames, msg2, duration=3600):
    start_time = datetime.now(timezone.utc)

    async def handler(event):
        if event.message.out or event.message.date <= start_time:
            return
        sender = await event.get_sender()
        username = getattr(sender, 'username', None)
        if username and username.lower() in usernames:
            await event.reply(msg2)
            usernames.remove(username.lower())

    client.add_event_handler(handler, events.NewMessage(incoming=True))
    try:
        await asyncio.sleep(duration)
    finally:
        client.remove_event_handler(handler)
        await client.disconnect()

# Храним состояние аккаунтов: ok или текст ошибки
account_status = {}
proxy_status = {}
available_chats = []
reply_tasks = []
# Глобальная блокировка для исключения одновременного доступа к сессиям
session_lock = asyncio.Lock()


def parse_proxy(proxy_str):
    parts = proxy_str.split(':')
    host = parts[0]
    port = int(parts[1])
    user = parts[2] if len(parts) > 2 else None
    password = parts[3] if len(parts) > 3 else None
    return (socks.SOCKS5, host, port, True, user, password)


async def get_proxy_map():
    sessions = await get_sessions()
    proxies = load_proxies()
    return {
        session: (proxies[i] if i < len(proxies) else None)
        for i, session in enumerate(sessions)
    }


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
        [Button.text('Статистика', resize=True), Button.text('Чаты')],
        [Button.text('Списки'), Button.text('Очистить пользователей')],
        [Button.text('Логи отправки'), Button.text('Сессии')],
        [
            Button.text('Добавить сессию'),
            Button.text('Добавить архив'),
            Button.text('Добавить прокси'),
        ],
        [Button.text('Пинг прокси'), Button.text('Скачать прокси')],
    ]
    await event.respond(
        'Выберите команду на клавиатуре ниже.\n'
        'Для команд с параметрами используйте ввод вручную:\n'
        '/set_message <текст> - текст рассылки\n'
        '/set_message1 <текст> - сообщение #1\n'
        '/set_message2 <текст> - сообщение #2\n'
        '/set_delay <сек> - задержка между сообщениями\n'
        '/add_user <username> - добавить пользователя\n'
        '/users <номер> - отправить список\n'
        '/parse <номер> - спарсить чат\n'
        '/del_list <номер> - удалить список\n'
        '/split <номер> <частей> - разделить список\n'
        '/test <username> - тестовая отправка\n'
        '/send <номер> - запустить рассылку\n'
        '/send_reply <номер> - рассылка с ответом\n'
        '/del_session <имя> - удалить сессию\n'
        '/add_session - добавить .session\n'
        '/add_zip - добавить .zip архив с сессиями\n'
        '/add_proxy <прокси> - заменить список прокси (несколько через перенос строки)\n'
        '/ping_proxy - проверить прокси\n'
        '/get_proxy - скачать список прокси',
        buttons=keyboard,
    )

@bot.on(events.NewMessage(pattern='/stats|Статистика'))
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


@bot.on(events.NewMessage(pattern='/sessions|Сессии'))
@notify_errors
async def list_sessions_cmd(event):
    proxy_map = await get_proxy_map()
    sessions = await get_sessions()
    if not sessions:
        await event.respond('Нет сессий')
        return
    lines = []
    now = datetime.utcnow()
    for s in sessions:
        p = proxy_map.get(s)
        if not p:
            emoji = '🔴'
            proxy_text = 'нет'
        else:
            info = proxy_status.get(s)
            if not info or (now - info['time']).total_seconds() > 300:
                emoji = '🟡'
            else:
                emoji = '🟢' if info['alive'] else '🔴'
            proxy_text = p
        lines.append(f"{emoji} {s} - {proxy_text}")
    await event.respond('Сессии:\n' + '\n'.join(lines))


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
        path = os.path.join(SESSIONS_DIR, name)
        if os.path.exists(path):
            os.remove(path)
            account_status.pop(name, None)
            await event.respond('Сессия удалена')
        else:
            await event.respond('Сессия не найдена')


@bot.on(events.NewMessage(pattern='/add_session|Добавить сессию'))
@notify_errors
async def add_session(event):
    # Если пришёл файл, обрабатываем его
    if event.message.file:
        filename = event.message.file.name

        # Один .session файл
        if filename.endswith('.session'):
            async with session_lock:
                dest = os.path.join(SESSIONS_DIR, filename)
                await event.message.download_media(file=dest)
            account_status[filename] = 'ok'
            await event.respond('Сессия добавлена')
            return

        # Предложить использовать другую команду для архива
        if filename.endswith('.zip'):
            await event.respond('Используйте команду /add_zip для импорта архива')
            return

    # Иначе запускаем интерактивное добавление через телефон
    async with bot.conversation(event.chat_id, timeout=120) as conv:
        await conv.send_message('Введите номер телефона в формате +79990000000')
        phone = (await conv.get_response()).raw_text.strip()
        session_name = phone.replace('+', '').replace(' ', '')
        client = TelegramClient(os.path.join(SESSIONS_DIR, session_name), api_id, api_hash)
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


@bot.on(events.NewMessage(pattern='/add_zip|Добавить архив'))
@notify_errors
async def add_zip(event):
    async def process(message, send):
        await send('Архив принят, обрабатывается...')
        data = BytesIO()
        await message.download_media(file=data)
        data.seek(0)
        async with session_lock:
            with zipfile.ZipFile(data) as zf:
                session_files = [n for n in zf.namelist() if n.endswith('.session')]
                if not session_files:
                    await send('Ошибка: .session файлы не найдены')
                    return
                await send(
                    f'Архив обработан, найдено сессий: {len(session_files)}. Импортирую...'
                )
                for name in session_files:
                    dest = os.path.join(SESSIONS_DIR, os.path.basename(name))
                    with zf.open(name) as src, open(dest, 'wb') as dst:
                        dst.write(src.read())
                    account_status[os.path.basename(name)] = 'ok'
        await send(f'Импортировано сессий: {len(session_files)}')

    # Если архив отправлен вместе с командой
    if event.message.file and event.message.file.name.endswith('.zip'):
        await process(event.message, event.respond)
        return
    async with bot.conversation(event.chat_id, timeout=60) as conv:
        await conv.send_message('Отправьте .zip архив с сессиями')
        response = await conv.get_response()
        if response.file and response.file.name.endswith('.zip'):
            await process(response, conv.send_message)
        else:
            await conv.send_message('Ошибка: архив не получен')


@bot.on(events.NewMessage(pattern='/add_proxy|Добавить прокси'))
@notify_errors
async def add_proxy(event):
    lines = event.raw_text.splitlines()[1:]
    if not lines:
        await event.respond('Отправьте /add_proxy и список прокси в следующих строках')
        return
    proxies = [line.strip() for line in lines if line.strip()]
    save_proxies(proxies)
    await event.respond(f'Добавлено прокси: {len(proxies)}')


@bot.on(events.NewMessage(pattern='/ping_proxy|Пинг прокси'))
@notify_errors
async def ping_proxy(event):
    proxy_map = await get_proxy_map()
    if not proxy_map:
        await event.respond('Нет сессий')
        return
    for session, p in proxy_map.items():
        if not p:
            proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
            continue
        proxy_conf = parse_proxy(p)
        client = TelegramClient(
            os.path.join(SESSIONS_DIR, session),
            api_id,
            api_hash,
            proxy=proxy_conf,
        )
        try:
            await client.connect()
            await client.get_me()
            proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
        except Exception:
            proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
        finally:
            await client.disconnect()
    await event.respond('Проверка прокси завершена')


@bot.on(events.NewMessage(pattern='/get_proxy|Скачать прокси'))
@notify_errors
async def get_proxy_list(event):
    proxies = load_proxies()
    if not proxies:
        await event.respond('Список прокси пуст')
        return
    await event.respond('Список прокси:', file=PROXY_FILE)

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


@bot.on(events.NewMessage(pattern='/set_message1'))
@notify_errors
async def set_message1(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /set_message1 текст')
        return
    with open(MESSAGE1_FILE, 'w') as f:
        f.write(parts[1])
    await event.respond('Сообщение #1 сохранено')


@bot.on(events.NewMessage(pattern='/set_message2'))
@notify_errors
async def set_message2(event):
    parts = event.raw_text.split(' ', 1)
    if len(parts) < 2:
        await event.respond('Использование: /set_message2 текст')
        return
    with open(MESSAGE2_FILE, 'w') as f:
        f.write(parts[1])
    await event.respond('Сообщение #2 сохранено')


@bot.on(events.NewMessage(pattern='/set_delay'))
@notify_errors
async def set_delay_cmd(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /set_delay секунды')
        return
    try:
        value = float(parts[1])
    except ValueError:
        await event.respond('Введите число секунд')
        return
    global message_delay
    message_delay = max(0, value)
    save_delay(message_delay)
    await event.respond(f'Задержка установлена: {message_delay} c')


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


@bot.on(events.NewMessage(pattern='/clear_users|Очистить пользователей'))
@notify_errors
async def clear_users(event):
    clear_user_lists()
    await event.respond('Все списки пользователей очищены')


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


@bot.on(events.NewMessage(pattern='/chats|Чаты'))
@notify_errors
async def list_chats(event):
    async with session_lock:
        proxy_map = await get_proxy_map()
        sessions = await get_sessions()
        if not sessions:
            await event.respond('Нет аккаунтов')
            return
        groups = []
        chat_map = []
        seen_ids = set()
        for session in sessions:
            proxy_str = proxy_map.get(session)
            if not proxy_str:
                continue
            proxy_conf = parse_proxy(proxy_str)
            async with TelegramClient(
                os.path.join(SESSIONS_DIR, session),
                api_id,
                api_hash,
                proxy=proxy_conf,
            ) as client:
                result = await client(
                    GetDialogsRequest(
                        offset_date=None,
                        offset_id=0,
                        offset_peer=InputPeerEmpty(),
                        limit=200,
                        hash=0,
                    )
                )
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
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
        proxy_map = await get_proxy_map()
        created_files = []
        for session, chat in targets:
            proxy_str = proxy_map.get(session)
            if not proxy_str:
                continue
            proxy_conf = parse_proxy(proxy_str)
            async with TelegramClient(
                os.path.join(SESSIONS_DIR, session),
                api_id,
                api_hash,
                proxy=proxy_conf,
            ) as client:
                participants = await client.get_participants(chat)
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
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


@bot.on(events.NewMessage(pattern='/lists|Списки'))
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
        proxy_map = await get_proxy_map()
        sessions = await get_sessions()
        session = None
        for s in sessions:
            if proxy_map.get(s):
                session = s
                break
        if not session:
            await event.respond('Нет аккаунтов с прокси')
            return
        with open(MESSAGE_FILE) as f:
            msg = f.read()
        proxy_conf = parse_proxy(proxy_map[session])
        async with TelegramClient(
            os.path.join(SESSIONS_DIR, session),
            api_id,
            api_hash,
            proxy=proxy_conf,
        ) as client:
            await client.send_message(parts[1], msg)
            proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
    await event.respond('Отправлено')

@bot.on(events.NewMessage(pattern=r'/send(?:\s|$)'))
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
        proxy_map = await get_proxy_map()
        sessions = await get_sessions()
        if not sessions:
            await event.respond('Нет аккаунтов')
            return
        with open(MESSAGE_FILE) as f:
            msg = f.read()

        clients = {}
        account_status.clear()
        for session in sessions:
            proxy_str = proxy_map.get(session)
            if not proxy_str:
                account_status[session] = 'no proxy'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
                continue
            client = TelegramClient(
                os.path.join(SESSIONS_DIR, session),
                api_id,
                api_hash,
                proxy=parse_proxy(proxy_str),
            )
            try:
                await client.start()
                clients[session] = client
                account_status[session] = 'ok'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
            except Exception as e:
                account_status[session] = f'error: {type(e).__name__}'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}

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
                    await asyncio.sleep(message_delay)
                    break
                except Exception as e:
                    await client.disconnect()
                    account_status[session] = f'error: {type(e).__name__}'
                    proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
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


@bot.on(events.NewMessage(pattern=r'/send_reply(?:\s|$)'))
@notify_errors
async def send_reply(event):
    parts = event.raw_text.split()
    if len(parts) < 2:
        await event.respond('Использование: /send_reply номер')
        return
    files = get_user_lists()
    try:
        fname = files[int(parts[1])]
    except (ValueError, IndexError):
        await event.respond('Неверный номер списка')
        return
    if not os.path.exists(MESSAGE1_FILE) or not os.path.exists(MESSAGE2_FILE):
        await event.respond('Сначала задайте тексты через /set_message1 и /set_message2')
        return
    with open(fname) as f:
        users = [u.strip() for u in f if u.strip()]
    if not users:
        await event.respond('Нет пользователей для рассылки')
        return
    for task in reply_tasks:
        task.cancel()
    reply_tasks.clear()

    async with session_lock:
        proxy_map = await get_proxy_map()
        sessions = await get_sessions()
        if not sessions:
            await event.respond('Нет аккаунтов')
            return
        with open(MESSAGE1_FILE) as f:
            msg1 = f.read()
        with open(MESSAGE2_FILE) as f:
            msg2 = f.read()

        clients = {}
        pending = {}
        account_status.clear()
        for session in sessions:
            proxy_str = proxy_map.get(session)
            if not proxy_str:
                account_status[session] = 'no proxy'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
                continue
            client = TelegramClient(
                os.path.join(SESSIONS_DIR, session),
                api_id,
                api_hash,
                proxy=parse_proxy(proxy_str),
            )
            try:
                await client.start()
                clients[session] = client
                pending[client] = set()
                account_status[session] = 'ok'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': True}
            except Exception as e:
                account_status[session] = f'error: {type(e).__name__}'
                proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}

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
            username_clean = user.lstrip('@')
            personalized = msg1.replace('[имя]', username_clean)
            while queue and attempts < len(queue):
                session, client = queue[0]
                try:
                    await client.send_message(user, personalized)
                    queue.rotate(-1)
                    delivered = True
                    log_lines.append(f'{user}: delivered')
                    pending[client].add(username_clean.lower())
                    await asyncio.sleep(message_delay)
                    break
                except Exception as e:
                    await client.disconnect()
                    account_status[session] = f'error: {type(e).__name__}'
                    proxy_status[session] = {'time': datetime.utcnow(), 'alive': False}
                    queue.popleft()
                    clients.pop(session, None)
                    attempts += 1
                    error_text = f'{type(e).__name__}: {e}'
            if not delivered:
                failed_users.append(user)
                log_lines.append(f'{user}: {error_text or "failed"}')

        for session, client in clients.items():
            usernames = pending.get(client, set())
            if usernames:
                task = asyncio.create_task(reply_watcher(client, usernames, msg2))
                reply_tasks.append(task)
            else:
                await client.disconnect()

        with open('send_log.txt', 'w') as log_file:
            log_file.write('\n'.join(log_lines))

        if failed_users:
            await event.respond('Не доставлено: ' + ', '.join(failed_users) + '\nОжидание ответов начато')
        else:
            await event.respond('Рассылка завершена. Ожидание ответов начато')

@bot.on(events.NewMessage(pattern='/end|Логи отправки'))
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
