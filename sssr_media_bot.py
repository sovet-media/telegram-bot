import asyncio
import logging
import random
import re

from telethon import TelegramClient, events, Button
from telethon.errors import MessageNotModifiedError
from telethon.utils import pack_bot_file_id

import database
import upload_tasker
from database import Category
from properties import API_ID, API_HASH, SSSR_SESSION, BOT_TOKEN, PROXY_TYPE, PROXY_ADDRESS, PROXY_PORT, PROXY_USERNAME, \
    PROXY_PASSWORD

MAX_BUTTON_COUNT = 7

asyncio.set_event_loop(asyncio.SelectorEventLoop())
bot = TelegramClient(SSSR_SESSION, API_ID, API_HASH, proxy=(PROXY_TYPE, PROXY_ADDRESS, PROXY_PORT, True, PROXY_USERNAME, PROXY_PASSWORD))

logging.basicConfig(level=logging.INFO)


@bot.on(events.NewMessage(from_users='KIZUF'))
async def handler(event):
    message = event.message
    video = message.video
    if video and message.text:
        sr = str(message.text).split(':')
        # category:id
        if len(sr) == 2:
            category = Category(sr[0])
            if category:
                id = int(sr[1])
                file_id = pack_bot_file_id(video)
                database.set_media_file_id(category, id, file_id)
                await bot.delete_messages(message.from_id, message)
                task = database.get_up_task(category, id)
                for chat in task.msgs:
                    chat = int(chat)
                    for msg_id in task.msgs:
                        print(f"/{type(chat)}//{type(msg_id)}//{type(message)}")
                        print(f"/{type(category)}//{type(id)}")
                        msg = await bot.get_messages(chat, ids=int(msg_id))
                        await bot.delete_messages(chat, msg)
                        await send_media(chat, category, id)


def select_category_menu(user_id):
    result = []
    selected_c = database.get_category(user_id)
    for category in (Category):
        result.append([Button.text(('🏇' if selected_c is category else ' ') + category.value)])
    return result


@bot.on(events.NewMessage())
async def handler(event):
    message = event.message
    user_id = message.from_id

    if message.text in [category.value for category in Category]:
        category = Category(message.text)
        database.set_category(user_id, category)
        await bot.send_message(user_id, "your search category changed to {}".format(category.value),
                               buttons=bot.build_reply_markup(select_category_menu(user_id)))
    else:
        selected_category = database.get_category(user_id) or Category.TALES
        result = database.search_media(selected_category, message.text)
        if len(result) > 0:
            await bot.send_message(user_id, 'search_result in ' + selected_category.value,
                                   buttons=search_pag_keyboard(selected_category, message.text, 0, result))


@bot.on(events.NewMessage(pattern="/start"))
async def handler(event):
    message = event.message
    user_id = message.from_id
    cat_menu = bot.build_reply_markup(select_category_menu(user_id))
    smiles = ['😘', '❤️', '😍', '😎', '😝', '🧐', '🤓', '😴', '🌈', '🌒']
    await bot.send_message(event.message.from_id, random.choice(smiles), buttons=cat_menu)


@bot.on(events.NewMessage(pattern="/file_id"))
async def handler(event):
    print(pack_bot_file_id(event.original_update.message.media))


@bot.on(events.CallbackQuery)
async def handler(event):
    data = event.data.decode("utf-8")
    match = re.compile('^command:(?P<command>.*?):.*?$').match(data)
    command = match.group('command')
    if command:
        update = event.original_update
        if command == 'search':
            match = re.compile('^command:search:(?P<category>.*?):\"(?P<query>.*?)\":(?P<offset>.*?)$').match(data)
            category = Category(match.group('category'))
            query = match.group('query')
            offset = int(match.group('offset'))
            await bot.edit_message(update.user_id, update.msg_id, buttons=search_pag_keyboard(category,
                                                                                              query,
                                                                                              offset,
                                                                                              database.search_media(
                                                                                                  category, query)))
        elif command == 'media':
            match = re.compile('^command:media:(?P<category>.*?):(?P<id>.*?)$').match(data)
            category = Category(match.group('category'))
            id = int(match.group('id'))
            # media = database.get_media(category, id)
            message = await bot.get_messages(update.user_id, ids=update.msg_id)
            await send_media(update.user_id, category, id, msg_id=update.msg_id if message.file else None)
        elif command == 'requests_to_upload':
            match = re.compile('^command:requests_to_upload:(?P<category>.*?):(?P<id>.*?)$').match(data)
            category = Category(match.group('category'))
            id = int(match.group('id'))
            upload_tasker.force_upload(category, id, {
                update.user_id: [update.msg_id]
            })


async def send_media(chat, category: Category, id: int, *, msg_id=None):
    media = database.get_media(category, id)
    file_id = telegram_cloud_contains_media_file(category, id)
    if file_id:
        file = file_id
        buttons = media_pag_keyboard(category, id)
    else:
        # file = 'assets/wait.jpg'
        file = 'AgADAgADvawxG8Ye8EnopCI4S88LCsV0wQ8ADDE7BAABAg'
        buttons = media_pag_keyboard(category, id,
                                     top_button=Button.inline('requests_to_upload',
                                                              'command:requests_to_upload:{}:{}'.format(
                                                                  category.value, id)))
    if msg_id:
        await bot.edit_message(chat, msg_id, media.name,
                               file=file,
                               buttons=buttons
                               )
    else:
        await bot.send_message(chat, media.name,
                               file=file,
                               buttons=buttons
                               )


def telegram_cloud_contains_media_file(category: Category, id: int):
    media = database.get_media(category, id)
    file_id = database.get_media_file_id(category, media.id)
    if file_id:
        return file_id
    else:
        return False


def search_pag_keyboard(category: Category, query, offset, media_list):
    keyboard_buttons = []
    for media in media_list[offset:offset + MAX_BUTTON_COUNT]:
        keyboard_buttons.append([
            Button.inline(media.name, "command:media:{}:{}".format(category.value, media.id))
        ])
    pagination_buttons = []
    if offset >= MAX_BUTTON_COUNT:
        pagination_buttons.append(
            Button.inline("<<", "command:search:{}:\"{}\":{}".format(
                category.value,
                query,
                offset - MAX_BUTTON_COUNT))
        )
    if len(media_list) - offset > MAX_BUTTON_COUNT:
        pagination_buttons.append(
            Button.inline(">>", "command:search:{}:\"{}\":{}".format(
                category.value,
                query,
                offset + MAX_BUTTON_COUNT))
        )
    keyboard_buttons.append(pagination_buttons)
    return keyboard_buttons


def media_pag_keyboard(category: Category, id: int, top_button: Button = None):
    pagination_buttons = []
    result = []
    if database.media_exits(category, id - 1):
        pagination_buttons.append(Button.inline('<<', 'command:media:{}:{}'.format(category.value, id - 1)))
    if database.media_exits(category, id + 1):
        pagination_buttons.append(Button.inline('>>', 'command:media:{}:{}'.format(category.value, id + 1)))
    if top_button:
        result.append([top_button])
    result.append(pagination_buttons)
    return result


def on_media_uploaded(category: Category, id: int, msgs):
    logging.info('{}: {} in {} is uploaded'.format(__name__, category, id))


dot_count = 3
next_dot = 1


async def on_progress_task(category: Category, id: int, task: database.UpTask):
    def _next_sym():
        global next_dot
        result = ''
        for i in range(next_dot):
            result += '.'
        if next_dot == dot_count:
            next_dot = 1
        else:
            next_dot += 1
        return result

    next_prog = _next_sym()
    for chat in task.msgs:
        for message in task.msgs[chat]:
            message = await bot.get_messages(int(chat), ids=message)
            media = database.get_media(category, id)
            if message.text == media.name:
                if task.state == database.UpTaskState.DOWNLOADING:
                    top_button = Button.inline('Downloading' + next_prog)
                elif task.state == database.UpTaskState.UPLOADING:
                    per = upload_tasker.get_task_progress(category, id)
                    etc = upload_tasker.get_task_etc_m(category, id)
                    top_button = Button.inline('Uploaded {}% (etc {}m)'.format(per, etc))
                try:
                    await bot.edit_message(int(chat), message, buttons=media_pag_keyboard(category, id, top_button))
                except MessageNotModifiedError:
                    logging.warning("huli-podelat?, content of the message was not modified")


def main():
    bot.start(bot_token=BOT_TOKEN)
    upload_tasker.init()
    bot.loop.create_task(upload_tasker.uploader_task(on_media_uploaded))
    bot.loop.create_task(upload_tasker.progress_task(on_progress_task))
    bot.run_until_disconnected()


if __name__ == "__main__":
    aio_loop = asyncio.get_event_loop()
    try:
        aio_loop.run_until_complete(main())
    finally:
        if not aio_loop.is_closed():
            aio_loop.close()
