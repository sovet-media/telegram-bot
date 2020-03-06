import asyncio
import logging
import random
import re

from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError
from telethon.utils import pack_bot_file_id

import database
import upload_tasker
from database import *
from properties import *

MAX_BUTTON_COUNT = 11

asyncio.set_event_loop(asyncio.SelectorEventLoop())
bot = TelegramClient(SSSR_SESSION, API_ID, API_HASH,
                     proxy=(PROXY_TYPE, PROXY_ADDRESS, PROXY_PORT, True, PROXY_USERNAME, PROXY_PASSWORD))

logging.basicConfig(level=logging.INFO)


@bot.on(events.NewMessage(from_users='KIZUF'))
async def handler(event):
    message_event = event.message
    message_text = message_event.text
    message_video = message_event.video
    if message_video and message_text:
        media_id = int(message_text)
        file_id = pack_bot_file_id(message_video)
        database.set_media_file_id(media_id, file_id)
        await bot.delete_messages(message_event.from_id, message_event)
        task = database.get_up_task(media_id)
        for chat_id in task.msgs:
            await bot.delete_messages(entity=chat_id,
                                      message_ids=task.msgs[chat_id])
            await send_media(chat_id, media_id)


@bot.on(events.NewMessage(pattern="/start"))
async def handler(event):
    await event.delete()
    message = event.message
    smiles = ['😘', '❤️', '😍', '😎', '😝', '🧐', '🤓', '😴', '🌈', '🌒']
    await bot.send_message(message.from_id, random.choice(smiles))


@bot.on(events.NewMessage(pattern='^[^/*]'))
async def handler(event):
    message = event.message
    user_id = message.from_id
    message_text = message.text
    result = database.search_media(message_text) if message_text else None
    await bot.send_message(user_id, 'Результаты поиска',
                           buttons=sorted_search_pag_keyboard(message_text, 0, result))


@bot.on(events.CallbackQuery)
async def handler(event):
    data = event.data.decode("utf-8")
    match = re.compile('^command:(?P<command>.*?):.*?$').match(data + ':')
    if match:
        command = match.group('command')
        if command:
            update = event.original_update
            if command == 'search':
                match = re.compile('^command:search:\"(?P<query>.*?)\":(?P<offset>.*?)$').match(data)
                query = match.group('query')
                offset = int(match.group('offset'))
                await bot.edit_message(update.user_id, update.msg_id, buttons=sorted_search_pag_keyboard(query,
                                                                                                         offset,
                                                                                                         database.search_media(
                                                                                                             query)))
            elif command == 'media':
                match = re.compile('^command:media:(?P<media_id>.*?)$').match(data)
                media_id = int(match.group('media_id'))
                message = await bot.get_messages(update.user_id, ids=update.msg_id)
                await send_media(update.user_id, media_id, msg_id=update.msg_id if message.file else None)
            elif command == 'requests_to_upload':
                match = re.compile('^command:requests_to_upload:(?P<media_id>.*?)$').match(data)
                media_id = int(match.group('media_id'))
                upload_tasker.force_upload(media_id, {
                    update.user_id: [update.msg_id]
                })
            elif command == 'delete_this':
                await bot.delete_messages(update.user_id, update.msg_id)


async def send_media(chat_id: int, id: int, *, msg_id=None):
    media = database.get_media(id)
    file_id = telegram_cloud_contains_media_file(id)
    if file_id:
        file = file_id
        buttons = media_pag_keyboard(id)
    else:
        # file = 'assets/wait.jpg'
        file = 'AgADAgADvawxG8Ye8EnopCI4S88LCsV0wQ8ADDE7BAABAg'
        buttons = media_pag_keyboard(id, top_button=Button.inline('Загрузить в телеграм',
                                                                  'command:requests_to_upload:{}'.format(id)))
    if msg_id:
        await bot.edit_message(chat_id, msg_id, media.name,
                               file=file,
                               buttons=buttons
                               )
    else:
        await bot.send_message(entity=chat_id,
                               message=media.name,
                               file=file,
                               buttons=buttons
                               )


def telegram_cloud_contains_media_file(id: int):
    media = database.get_media(id)
    file_id = database.get_media_file_id(media.id)
    if file_id:
        return file_id
    else:
        return False


def sorted_search_pag_keyboard(query: str, offset, media_list: List[database.Media]):
    media_list.sort(key=lambda media: media.name)
    return search_pag_keyboard(query, offset, media_list)


def search_pag_keyboard(query: str, offset, media_list: List[database.Media]) -> List[List[Button]]:
    keyboard_buttons = []
    if media_list:
        for media in media_list[offset:offset + MAX_BUTTON_COUNT]:
            keyboard_buttons.append([
                Button.inline(media.name, "command:media:{}".format(media.id))
            ])
        pagination_buttons = []
        if offset >= MAX_BUTTON_COUNT:
            pagination_buttons.append(
                Button.inline("<<", "command:search:\"{}\":{}".format(
                    query,
                    offset - MAX_BUTTON_COUNT))
            )
        if len(media_list) - offset > MAX_BUTTON_COUNT:
            pagination_buttons.append(
                Button.inline(">>", "command:search:\"{}\":{}".format(
                    query,
                    offset + MAX_BUTTON_COUNT))
            )
        keyboard_buttons.append(pagination_buttons)
    else:
        keyboard_buttons.append([Button.inline('{}, {}.'.format(
            random.choice(['Пустенько', 'Чистенько']),
            random.choice(['понюхайте', 'попробуйте', 'только без паники 🙃'])
        ), 'command:delete_this')])
    return keyboard_buttons


def media_pag_keyboard(media_id, top_button: Button = None):
    pagination_buttons = []
    result = []
    if database.media_exits(media_id - 1):
        pagination_buttons.append(Button.inline('<<', 'command:media:{}'.format(media_id - 1)))
    if database.media_exits(media_id + 1):
        pagination_buttons.append(Button.inline('>>', 'command:media:{}'.format(media_id + 1)))
    if top_button:
        result.append([top_button])
    result.append(pagination_buttons)
    return result


dot_count = 3
next_dot = 1


async def on_progress_task(media_id, task: database.UpTask):
    for chat_id in task.msgs:
        for message_id in task.msgs[chat_id]:
            message = await bot.get_messages(chat_id, ids=message_id)
            if message:
                message_text = message.text
                task_state = task.state
                media = database.get_media(media_id)
                if message_text == media.name:
                    if task_state == UpTaskState.DOWNLOADING:
                        top_button = Button.inline('Загрузка{}'.format('.' * random.randrange(1, 5, 1)))
                    elif task_state == UpTaskState.UPLOADING:
                        per = upload_tasker.get_task_progress(media_id)
                        etc = upload_tasker.get_task_etc_m(media_id)
                        top_button = Button.inline('Отправлено {}% (etc {}m)'.format(per, etc))
                    try:
                        await bot.edit_message(entity=message, buttons=media_pag_keyboard(media_id, top_button))
                    except MessageNotModifiedError:
                        logging.warning("new text == current text of message")


def main():
    bot.start(bot_token=BOT_TOKEN)
    upload_tasker.init()
    bot.loop.create_task(upload_tasker.task_reactor())
    bot.loop.create_task(upload_tasker.progress_task(on_progress_task))
    bot.run_until_disconnected()


if __name__ == "__main__":
    aio_loop = asyncio.get_event_loop()
    try:
        aio_loop.run_until_complete(main())
    finally:
        if not aio_loop.is_closed():
            aio_loop.close()
