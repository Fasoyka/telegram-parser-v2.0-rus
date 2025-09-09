'''
This Source Code Form is subject to the terms of the Mozilla
Public License, v. 2.0. If a copy of the MPL was not distributed
with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
'''

from telethon.sync import TelegramClient
from telethon.tl.types import ChannelParticipantsAdmins
import os
import time


def _remove_admins_and_mods(client, index, participants):
    """Remove administrators and moderators from participant list."""
    admins = client.get_participants(index, filter=ChannelParticipantsAdmins)
    admin_ids = {admin.id for admin in admins}
    return [user for user in participants if user.id not in admin_ids]

def parsing(client, index: int, id: bool, name: bool):
    all_participants = []
    all_participants = client.get_participants(index)
    all_participants = _remove_admins_and_mods(client, index, all_participants)
    if name:
        with open('usernames.txt', 'r+') as f:
            usernames = f.readlines()
            for user in all_participants:
                if user.username:
                    if ('Bot' not in user.username) and ('bot' not in user.username):
                        if (('@' + user.username + '\n') not in usernames):
                            f.write('@' + user.username + '\n')
                        else:
                            continue
                    else:
                        continue
                else:
                    continue
    if id:
        with open('userids.txt', 'r+') as f:
            userids = f.readlines()
            for user in all_participants:
                if (str(user.id) + '\n') not in userids:
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
                          "7 - Сбросить настройки\n"
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
