import argparse
import asyncio
import datetime
import json
import logging
import pprint
import shutil
import subprocess

from PIL import Image
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeVideo

import database
import upload_tasker
from database import UpTaskState
from properties import *

argparse = argparse.ArgumentParser()
argparse.add_argument("-id", type=int)
argparse.add_argument("-to")
args = argparse.parse_args()

# CHUNK_SIZE = 4 * 1024

logging.basicConfig(level=logging.INFO)

asyncio.set_event_loop(asyncio.SelectorEventLoop())

current_session_name = '{}/{}'.format(AGENT_SESSION, args.id)
shutil.copyfile('./{}.session'.format(AGENT_E_SESSION), './{}.session'.format(current_session_name))
telegram_client = TelegramClient(current_session_name, API_ID, API_HASH, proxy=(
    PROXY_TYPE, PROXY_ADDRESS, PROXY_PORT, True, PROXY_USERNAME, PROXY_PASSWORD)).start()


def get_local_path(file_name):
    return 'converted/{}'.format(file_name)


def download_and_convert(url, save_to):
    subprocess.check_call(['ffmpeg', '-y',
                           '-nostats',
                           '-loglevel', '0',
                           '-i', url,
                           save_to])


def get_video_metadata(path):
    return json.loads(subprocess.check_output(['ffprobe',
                                               '-v', 'quiet',
                                               '-print_format', 'json',
                                               '-show_streams',
                                               path
                                               ]).decode('utf-8'))


def get_video_dwh(path):
    metadata = get_video_metadata(path)
    duration = metadata['streams'][0]['duration']
    width = metadata['streams'][0]['width']
    height = metadata['streams'][0]['height']
    logging.info("info of '{}':\n . width and height = {}, {}\n . duration = {}".format(path, width, height, duration))
    return int(float(duration)), int(width), int(height)


def get_video_thumb(file, path):
    subprocess.check_output(['ffmpeg', '-y',
                             '-nostats',
                             '-loglevel', '0',
                             '-i', file,
                             '-ss', '00:00:07.000',
                             '-vframes', '1',
                             path])
    thumb = Image.open(path)
    thumb.thumbnail((200, 200), reducing_gap=3.0)
    thumb.save(path)
    return path


async def main():
    media = database.get_media(args.id)
    pprint.pprint(media)
    file_name = media.name.replace(" ", "_")
    converted_video_path = get_local_path('{}_converted.{}'.format(file_name, 'mp4'))
    thumb_save_to = get_local_path('{}.{}'.format(file_name, 'jpg'))
    upload_tasker.set_state_of_up_task(args.id, UpTaskState.DOWNLOADING)

    try:
        download_and_convert(media.http_url, converted_video_path)
        duration, width, height = get_video_dwh(converted_video_path)
        get_video_thumb(converted_video_path, thumb_save_to)
        upload_tasker.set_state_of_up_task(args.id, UpTaskState.UPLOADING)

        started_at = datetime.datetime.now()

        def progress_ink(send, total):
            file_percent = total * .01
            uploaded_percents = round(send / file_percent, 2)

            uploading_minutes = (datetime.datetime.now() - started_at).seconds / 60
            upload_per_minute = send / uploading_minutes
            eta_in_minutes = round((total - send) / upload_per_minute, 2)

            upload_tasker.set_task_etc_m(args.id, eta_in_minutes)
            upload_tasker.set_task_progress(args.id, uploaded_percents)

        await telegram_client.send_file(
            args.to,
            converted_video_path,
            # category:id
            caption=str(args.id),
            attributes=[DocumentAttributeVideo(duration, width, height, supports_streaming=True)],
            thumb=thumb_save_to,
            progress_callback=progress_ink
        )
        upload_tasker.set_state_of_up_task(args.id, UpTaskState.SUCCESS)
        # os.remove(converted_video_save_to)
    except Exception as e:
        upload_tasker.set_state_of_up_task(args.id, UpTaskState.FAIL)
        logging.error('(ex)stopped downloading {}'.format(media.name))
        logging.error('{}: \n{}'.format(e.__class__, e))
    await telegram_client.disconnect()


if __name__ == "__main__":
    aio_loop = asyncio.get_event_loop()
    try:
        aio_loop.run_until_complete(main())
    finally:
        if not aio_loop.is_closed():
            aio_loop.close()
