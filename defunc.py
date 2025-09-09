'''
This Source Code Form is subject to the terms of the Mozilla
Public License, v. 2.0. If a copy of the MPL was not distributed
with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
'''

from telethon.sync import TelegramClient
from telethon.tl.types import (
    ChannelParticipantsAdmins,
    ChannelParticipantAdmin,
    ChannelParticipantCreator,
    ChatParticipantAdmin,
    ChatParticipantCreator,
)
import os
import time
import re
import zipfile
from datetime import datetime


LISTS_DIR = 'lists'
os.makedirs(LISTS_DIR, exist_ok=True)


def _remove_admins_and_mods(client, index, participants):
    """Remove owners, moderators and editors from participant list."""
    admins = client.get_participants(index, filter=ChannelParticipantsAdmins)
    admin_ids = {admin.id for admin in admins}
    filtered = []
    for user in participants:
        if user.id in admin_ids:
            continue
        try:
            perms = client.get_permissions(index, user.id)
            if isinstance(
                perms,
                (
                    ChannelParticipantAdmin,
                    ChannelParticipantCreator,
                    ChatParticipantAdmin,
                    ChatParticipantCreator,
                ),
            ):
                continue
        except Exception:
            pass
        filtered.append(user)
    return filtered

def parsing(client, index: int, id: bool, name: bool):
    """Parse participants of a chat and save to uniquely named files."""
    all_participants = client.get_participants(index)
    all_participants = _remove_admins_and_mods(client, index, all_participants)

    prefix = re.sub(r"\W+", "_", getattr(index, 'title', 'chat'))[:10]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if name:
        name_file = os.path.join(LISTS_DIR, f"users_{prefix}_{timestamp}.txt")
        with open(name_file, 'w', encoding='utf-8') as f:
            for user in all_participants:
                if user.username and 'bot' not in user.username.lower():
                    f.write('@' + user.username + '\n')

    if id:
        id_file = os.path.join(LISTS_DIR, f"ids_{prefix}_{timestamp}.txt")
        with open(id_file, 'w', encoding='utf-8') as f:
            for user in all_participants:
                f.write(str(user.id) + '\n')


def config():
    while True:
        os.system('cls||clear')

        with open('options.txt', 'r+') as f:
            if not f.readlines():
                f.write("NONEID\n"
                        "NONEHASH\n"
                        "True\n"
                        "True\n"
                        "NONETOKEN\n")
                continue
                
        options = getoptions()
        sessions = []
        for file in os.listdir('.'):
            if file.endswith('.session'):
                sessions.append(file)

        key = str(input((f"1 - Обновить api_id [{options[0].replace('\n', '')}]\n"
                         f"2 - Обновить api_hash [{options[1].replace('\n', '')}]\n"
                         f"3 - Парсить user-id [{options[2].replace('\n', '')}]\n"
                         f"4 - Парсить user-name [{options[3].replace('\n', '')}]\n"
                         f"5 - Обновить bot_token [{options[4].replace('\n', '')}]\n"
                         f"6 - Добавить аккаунт юзербота[{len(sessions)}]\n"
                         "7 - Массово добавить аккаунты (.zip)\n"
                         "8 - Сбросить настройки\n"
                         "e - Выход\n"
                         "Ввод: ")
                    ))

        if key == '1':
            os.system('cls||clear')
            options[0] = str(input("Введите API_ID: ")) + "\n"

        elif key == '2':
            os.system('cls||clear')
            options[1] = str(input("Введите API_HASH: ")) + "\n"

        elif key == '3':
            if options[2] == 'True\n':
                options[2] = 'False\n'
            else:
                options[2] = 'True\n'

        elif key == '4':
            if options[3] == 'True\n':
                options[3] = 'False\n'
            else:
                options[3] = 'True\n'
        
        elif key == '5':
            os.system('cls||clear')
            options[4] = str(input("Введите bot_token: ")) + "\n"

        elif key == '6':
            os.system('cls||clear')
            if options[0] == "NONEID\n" or options[1] == "NONEHASH\n":
                print("Проверьте api_id и api_hash")
                time.sleep(2)
                continue

            print("Аккаунты:\n")
            for i in sessions:
                print(i)

            phone = str(input("Введите номер телефона аккаунта: "))
            TelegramClient(phone, int(options[0].replace('\n', '')),
                                    options[1].replace('\n', '')).start(phone)

        elif key == '7':
            os.system('cls||clear')
            zip_path = input("Введите путь к .zip файлу с .session: ").strip()
            if not (zip_path.endswith('.zip') and os.path.isfile(zip_path)):
                print('Файл не найден или имеет неверное расширение.')
                time.sleep(2)
                continue
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    session_files = [f for f in zf.namelist() if f.endswith('.session')]
                    for member in session_files:
                        with zf.open(member) as src, open(os.path.basename(member), 'wb') as dst:
                            dst.write(src.read())
                if session_files:
                    print(f"Добавлено {len(session_files)} сессий.")
                else:
                    print('В архиве нет .session файлов.')
            except Exception as e:
                print(f'Ошибка распаковки: {e}')
            time.sleep(2)

        elif key == '8':
            os.system('cls||clear')
            answer = input("Вы уверены?\nAPI_ID и API_HASH будут удалены\n"
                           "1 - Удалить\n2 - Назад\n"
                           "Ввод: ")
            if answer == '1':
                options = []
                print("Настройки очищены.")
                time.sleep(2)
            else:
                continue

        elif key == 'e':
            os.system('cls||clear')
            break

        with open('options.txt', 'w') as f:
            f.writelines(options)


def getoptions():
    with open('options.txt', 'r') as f:
        options = f.readlines()
    return options


def clear_user_lists():
    """Remove all files inside the lists directory."""
    for filename in os.listdir(LISTS_DIR):
        file_path = os.path.join(LISTS_DIR, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
