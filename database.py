import json
import sys
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Any

import psycopg2
from psycopg2._psycopg import Error, DatabaseError
from psycopg2.extras import DictCursor

from properties import *

try:
    connect = psycopg2.connect(
        host=DATABASE_HOST,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        dbname=DATABASE_NAME
    )
    connect.autocommit = True
    cursor = connect.cursor(cursor_factory=DictCursor)
except DatabaseError as err:
    print(err.pgerror)
    print('not connected to database')
    sys.exit(1)
else:
    print('connected to database')


@dataclass
class Media:
    id: int
    name: str
    http_url: str


class UpTaskState(Enum):
    NONE = 'none'
    DOWNLOADING = 'downloading'
    UPLOADING = 'uploading'
    SUCCESS = 'success'
    FAIL = 'fail'


@dataclass
class UpTask:
    media_id: int
    msgs: dict
    state: Optional[UpTaskState]


def _media_from_row(row) -> Media:
    return Media(int(row['id']), row['name'], row['url'])


def _uptask_from_row(row) -> UpTask:
    msgs = dict([(int(chat_id), row['data']['msgs'][chat_id]) for chat_id in row['data']['msgs']]) if 'msgs' in row[
        'data'] else dict()
    return UpTask(row['media_id'], msgs, UpTaskState(row['state']))


def media_exits(media_id) -> bool:
    cursor.execute('SELECT id FROM media WHERE id = %s', (media_id,))
    return cursor.rowcount > 0


def search_media(query) -> List[Media]:
    try:
        cursor.execute('SELECT * FROM media WHERE to_tsvector(name) @@ plainto_tsquery(%s)', (query,))
        if cursor.rowcount > 0:
            return list(map(lambda row: _media_from_row(row), cursor))
    except DatabaseError as err:
        print(err.pgerror)
    return []


def set_media_file_id(media_id, file_id):
    cursor.execute('UPDATE media SET tg_file_id = %s WHERE id = %s', (file_id, media_id))


def get_media_file_id(media_id) -> str:
    try:
        cursor.execute('SELECT tg_file_id FROM media WHERE id = %s LIMIT 1', (media_id,))
        if cursor.rowcount > 0:
            for row in cursor:
                return row['tg_file_id']
    except Error as err:
        print(err.pgerror)
    return ""


def get_media(media_id) -> Optional[Media]:
    try:
        cursor.execute('SELECT * FROM media WHERE id = %s LIMIT 1', (media_id,))
        if cursor.rowcount > 0:
            for row in cursor:
                return _media_from_row(row)
    except Error as err:
        print(err.pgerror)
    return


def up_task_exits(media_id) -> bool:
    cursor.execute('SELECT id FROM task WHERE media_id = %s', (media_id,))
    return cursor.rowcount > 0


def get_up_task(media_id) -> Optional[UpTask]:
    try:
        cursor.execute('SELECT * from task WHERE media_id = %s LIMIT 1', (media_id,))
        if cursor.rowcount > 0:
            for row in cursor:
                return _uptask_from_row(row)
    except Error as err:
        print(err.pgerror)
    return


def set_up_task_state(media_id, state: UpTaskState):
    cursor.execute('UPDATE task SET state = %s WHERE media_id = %s', (state.value, media_id))


def add_up_task(media_id, msgs: dict):
    if up_task_exits(media_id):
        add_msgs_to_up_task(media_id, msgs)
    else:
        cursor.execute('INSERT INTO task (media_id, state, data) VALUES (%s, %s, %s)',
                       (media_id, UpTaskState.NONE.value, json.dumps({"msgs": msgs})))


def add_msgs_to_up_task(media_id, msgs: dict):
    for chat in msgs:
        jsonb_path = '{{msgs, {}}}'.format(chat)
        cursor.execute(
            "UPDATE task SET data = jsonb_set(data, %s, (data#>>%s)::jsonb || %s::jsonb) WHERE media_id = %s",
            (jsonb_path, jsonb_path, json.dumps(msgs[chat]), media_id))


def get_tasks() -> List[UpTask]:
    try:
        cursor.execute('SELECT * FROM task')
        if cursor.rowcount > 0:
            return list(map(lambda row: _uptask_from_row(row), cursor))
    except Error as err:
        print(err.pgerror)
    return []


def remove_up_task(media_id):
    cursor.execute('DELETE FROM task WHERE media_id = %s', (media_id,))


def set_attr_task(media_id, attr_path: str, value):
    cursor.execute(
        "UPDATE task SET data = data || (CONCAT('{\"attr\":', COALESCE(data->'attr', '{}'), '}'))::jsonb WHERE media_id = %s; UPDATE task SET data = jsonb_set(data, %s, %s) WHERE media_id = %s;",
        (media_id, "{{attr, {}}}".format(attr_path), json.dumps(value), media_id))


def get_attr_task(media_id, attr_path: str) -> Any:
    try:
        cursor.execute('SELECT data#>>%s FROM task WHERE media_id = %s', ('{{attr, {}}}'.format(attr_path), media_id))
        if cursor.rowcount > 0:
            for row in cursor:
                return row[0]
    except Error as err:
        print(err.pgerror)
    return


if __name__ == '__main__':
    media_id = 9451
    set_attr_task(media_id, 'progress', '11')
    set_attr_task(media_id, 'progress', '33')
    set_attr_task(media_id, 'ex', json.dumps({"msg": "error_message", "type": "FailWhenDownload"}))


def user_exits(user_id) -> bool:
    cursor.execute('SELECT id FROM user WHERE id = %s', (user_id,))
    return cursor.rowcount > 0


def add_user(user_id):
    if not user_exits(user_id):
        cursor.execute('INSERT INTO user (tg_user_id) VALUES (%s)', (user_id,))
