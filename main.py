'''
This Source Code Form is subject to the terms of the Mozilla
Public License, v. 2.0. If a copy of the MPL was not distributed
with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
'''

from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
from defunc import *
import time
import os

LISTS_DIR = 'lists'
os.makedirs(LISTS_DIR, exist_ok=True)

if __name__ == "__main__":
    while True:
        options = getoptions()
        if not options or options[0] == "NONEID\n" or options[1] == "NONEHASH\n":
            print("Добавьте API_ID и API_HASH")
            time.sleep(2)
            config()
            continue
        
        api_id = int(options[0].replace('\n', ''))
        api_hash = str(options[1].replace('\n', ''))
        if options[2] == 'True\n':
            user_id = True
        else:
            user_id = False
        if options[3] == 'True\n':
            user_name = True
        else:
            user_name = False

        os.system('cls||clear')
        selection = str(input("1 - Настройки\n"
                            "2 - Парсинг\n"
                            "3 - Управление ботом\n"
                            "e - Выход\n"
                            "Ввод: "))
        

        if selection == '1':
            config()


        elif selection == '2':
            chats = []
            last_date = None    
            size_chats = 200
            groups = []         

            print("Выберите юзер-бота для парсинга.\n"
                "(Аккаунт который состоит в группах, которые нужно спарсить)\n")

            sessions = []
            for file in os.listdir('.'):
                if file.endswith('.session'):
                    sessions.append(file)


            for i in range(len(sessions)):
                print(f"[{i}] -", sessions[i], '\n')
            i = int(input("Ввод: "))
            
            client = TelegramClient(sessions[i].replace('\n', ''), api_id, api_hash).start(sessions[i].replace('\n', ''))

            result = client(GetDialogsRequest(
                offset_date=last_date,
                offset_id=0,
                offset_peer=InputPeerEmpty(),
                limit=size_chats,
                hash=0
            ))
            chats.extend(result.chats)

            for chat in chats:
                try:
                    if chat.megagroup is True:
                        groups.append(chat)         
                except:
                    continue

            i = 0
            print('Очистка базы юзеров: clear') 
            print('-----------------------------')
            for g in groups:
                print(str(i) + ' - ' + g.title)
                i+=1
            print(str(i + 1) + ' - ' + 'Спарсить всё')
            g_index = str(input())

            if g_index == 'clear':
                open(os.path.join(LISTS_DIR, 'usernames.txt'), 'w').close()
                open(os.path.join(LISTS_DIR, 'userids.txt'), 'w').close()

            elif int(g_index) < i + 1:
                target_group = groups[int(g_index)]
                parsing(client, target_group, user_id, user_name)
                print('Спаршено.')

            elif int(g_index) == i + 1:
                for g_index in groups:
                    parsing(client, g_index, user_id, user_name)
                print('Спаршено.')

            

        elif selection == '3':
            os.system('python bot_manager.py')

        elif selection == 'e':
            break
