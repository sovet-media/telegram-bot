import sys
from asyncio import sleep
from subprocess import Popen

import database
from database import UpTaskState
from properties import SSSR_BOT_NAME


def get_up_task(id: int):
    return database.get_up_task(id)


def add_up_task(id: int, msgs: dict):
    database.add_up_task(id, msgs)


def get_up_tasks():
    return database.get_tasks()


def exits_up_task(id: int):
    return database.up_task_exits(id)


def set_state_of_up_task(id: int, state: UpTaskState):
    database.set_up_task_state(id, state)


def remove_up_task(id: int):
    database.remove_up_task(id)


def set_task_etc_m(id: int, minutes):
    database.set_attr_task(id, 'etc_m', minutes)


def get_task_etc_m(id: int) -> int:
    return database.get_attr_task(id, 'etc_m') or 0.00


def set_task_progress(id: int, percent):
    return database.set_attr_task(id, 'progress_p', percent)


def get_task_progress(id: int) -> int:
    return database.get_attr_task(id, 'progress_p') or 0.00


def init():
    for task in get_up_tasks():
        if task.state != UpTaskState.SUCCESS:
            set_state_of_up_task(task.media_id, UpTaskState.NONE)
            print('set task({}) state to {}'.format(task.media_id, UpTaskState.NONE))


def upload_media(media_id):
    set_state_of_up_task(media_id, UpTaskState.DOWNLOADING)
    Popen(['venv/Scripts/python.exe', 'telegram_agent.py',
           '-id', str(media_id),
           '-to', SSSR_BOT_NAME])


def force_upload(media_id, msgs: dict = {}):
    add_up_task(media_id, msgs)
    task = database.get_up_task(media_id)
    state = task.state
    if state in [UpTaskState.FAIL, UpTaskState.NONE]:
        upload_media(media_id)
        print('force_upload: starting for media({})'.format(media_id))


async def progress_task(one_progress):
    while True:
        try:
            for task in get_up_tasks():
                state = task.state
                if not state in [UpTaskState.SUCCESS, UpTaskState.FAIL]:
                    await one_progress(task.media_id, task)
        except Exception as e:
            print("{}: {}, {}".format(type(e), sys.exc_info(), e))
        await sleep(1 * 10)


MAX_TO_DOWNLOAD = 11


async def task_reactor():
    while True:
        tasks = database.get_tasks()
        print('task_reactor: init')
        count_of_in_progress_tasks = 0
        for task_i, task in enumerate(tasks):
            state = task.state
            if state == UpTaskState.SUCCESS:
                print('task_reactor: media({}) published'.format(task.media_id, count_of_in_progress_tasks))
                del tasks[task_i]
                remove_up_task(task.media_id)
            elif state in [UpTaskState.UPLOADING, UpTaskState.DOWNLOADING]:
                count_of_in_progress_tasks += 1
        print('task_reactor: {} tasks currently in progress'.format(count_of_in_progress_tasks))
        empty_slots_for_upload = MAX_TO_DOWNLOAD - count_of_in_progress_tasks
        print('task_reactor: {} empty slots for upload'.format(empty_slots_for_upload))
        if empty_slots_for_upload > 0:
            tasks_added = 0
            tasks.sort(key=lambda t: len(t.msgs))
            for task in tasks:
                if empty_slots_for_upload > 0:
                    if task.state in [UpTaskState.NONE, UpTaskState.FAIL]:
                        force_upload(task.media_id)
                        empty_slots_for_upload -= 1
                        tasks_added += 1
                else:
                    break
            print('task_reactor: {} tasks added'.format(tasks_added))
        await sleep(3 * 60)
