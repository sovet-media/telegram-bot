import logging
from asyncio import sleep
from subprocess import Popen

import database
from database import Category, UpTaskState
from properties import SSSR_BOT_NAME


def get_up_task(category: Category, id: int):
    return database.get_up_task(category, id)


def add_up_task(category: Category, id: int, msgs: dict = []):
    database.add_up_task(category, id, msgs)


def get_up_tasks(category: Category):
    return database.get_tasks(category)


def exits_up_task(category: Category, id: int):
    return database.up_task_exits(category, id)


def set_state_of_up_task(category: Category, id: int, state: UpTaskState):
    database.set_up_task_state(category, id, state)


def remove_up_task(category: Category, id: int):
    database.remove_up_task(category, id)


def set_task_etc_m(category: Category, id: int, minutes):
    database.set_attr_task(category, id, 'etc_m', minutes)


def get_task_etc_m(category: Category, id: int):
    return database.get_attr_task(category, id, 'etc_m') or 0.00


def set_task_progress(category: Category, id: int, percent):
    return database.set_attr_task(category, id, 'progress_p', percent)


def get_task_progress(category: Category, id: int):
    return database.get_attr_task(category, id, 'progress_p') or 0.00


# TODO: signed every category by wight
FIXED_W = 999
MAX_DOWNLOADS = 1
downloads = 0


def init():
    for category in list(map(Category, Category)):
        for task in get_up_tasks(category):
            set_state_of_up_task(category, task.id, None)


def upload_media(category: Category, id: int):
    set_state_of_up_task(category, id, UpTaskState.DOWNLOADING)
    Popen(['venv/Scripts/python.exe', 'telegram_agent.py',
           '-category', category.value,
           '-id', str(id),
           '-to', SSSR_BOT_NAME])
    logging.info('{}: uploading ({}, {}) started'.format(__name__, category, id))


def force_upload(category: Category, id: int, msgs: dict = {}):
    global downloads
    if MAX_DOWNLOADS > downloads:
        add_up_task(category, id, msgs)
        task = get_up_task(category, id)
        # if not task.state or task.state == UpTaskState.FAIL:
        if not task.state or task.state == UpTaskState.FAIL:
            logging.info('{}: forsed uploading ({}, {}) started'.format(__name__, category, id))
            upload_media(category, task.id)
            downloads += 1
    else:
        logging.info('{}: forsed uploading ({}, {}) not started(limit)'.format(__name__, category, id))


async def progress_task(one_progress):
    while True:
        try:
            for category in list(map(Category, Category)):
                for task in get_up_tasks(category):
                    if task.state != UpTaskState.SUCCESS and task.state != UpTaskState.FAIL:
                        await one_progress(category, task.id, task)
        except:
            pass
        await sleep(1)


async def uploader_task(on_uploaded):
    global downloads
    while True:
        downloads = 0
        for category in list(map(Category, Category)):
            in_progress = 0
            tasks = get_up_tasks(category)
            for task in tasks:
                if task.state == UpTaskState.SUCCESS:
                    logging.info('{}: media ({}, {}) is done'.format(__name__, category, task.id))
                    remove_up_task(category, task.id)
                    tasks.remove(task)
                    on_uploaded(category, task.id, task.msgs)
                elif task.state == UpTaskState.FAIL:
                    logging.error('{}: upload fail ({}, {})'.format(__name__, category, id))
                elif task.state:
                    in_progress += 1
            if in_progress > 0:
                downloads += in_progress
                logging.info('{}: in {} in progress = {}'.format(__name__, category, in_progress))
            uu = FIXED_W - in_progress
            if uu > 0:
                tasks.sort(key=lambda t: len(t.msgs))
                for task in tasks:
                    if not task.state or task.state == UpTaskState.FAIL:
                        force_upload(category, task.id)
                    uu -= 1
                    if uu == 0:
                        break
        await sleep(1 * 60)
