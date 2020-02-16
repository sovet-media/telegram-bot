import re
import string
from dataclasses import dataclass
from enum import Enum

from tinydb import TinyDB, Query, where, JSONStorage

from properties import DATABASE_CONTAINER

media_db = TinyDB("database/media_database.json")
up_task_db = TinyDB("database/task_database.json")
usr_db = TinyDB(DATABASE_CONTAINER + '/user_database.json').table('users')


class Category(Enum):
    FILMS = "films"
    TALES = "tales"
    MOVIES_FOR_CHILD = "movies-for-child"


@dataclass
class Media:
    id: int
    name: str
    http_url: str


class UpTaskState(Enum):
    DOWNLOADING = 'downloading'
    UPLOADING = 'uploading'
    SUCCESS = 'success'
    FAIL = 'fail'


@dataclass
class UpTask:
    id: int
    msgs: list
    state: UpTaskState = None


def media_exits(category: Category, id: int):
    return media_db.table(category.value).contains(Query().id == id)


table = str.maketrans('', '', string.punctuation)


def search_media(category: Category, query: str):
    result = []
    for r in media_db.table(category.value).search(Query().name.test(
            lambda name: query.lower() in name.translate(table).lower()
    )):
        result.append(Media(r["id"], r["name"], r["http_url"]))
    return sorted(result, key=lambda m: m.name)


def set_media_file_id(category: Category, id: int, file_id: int):
    table = media_db.table(category.value)
    table.upsert({"file_id": file_id}, Query().id == id)


def get_media_file_id(category: Category, id: int):
    table = media_db.table(category.value)
    result = table.search(Query().id == id)
    if len(result) > 0 and 'file_id' in result[0]:
        return result[0]['file_id']
    return


def get_media(category: Category, id: int):
    result = media_db.table(category.value).search(Query().id == id)[0]
    return Media(result["id"], result["name"], result["http_url"])


def up_task_exits(category: Category, id: int):
    return up_task_db.table(category.value).contains(Query().id == id)


def get_up_task(category: Category, id: int, ):
    table = up_task_db.table(category.value)
    row = table.search(Query().id == id)[0]
    return UpTask(row['id'], row['msgs'], UpTaskState(row['state']) if row['state'] else None)


def set_up_task_state(category: Category, id: int, state: UpTaskState):
    table = up_task_db.table(category.value)
    table.update({
        'state': state.value if state else None
    }, Query().id == id)


def add_up_task(category: Category, id: int, msgs: dict = {}):
    table = up_task_db.table(category.value)
    if up_task_exits(category, id):
        add_msg_to_up_task(category, id, msgs)
    else:
        table.insert({
            'id': id,
            'msgs': msgs,
            'state': None,
            'attrs': {}
        })


def add_msg_to_up_task(category: Category, id: int, msgs: dict = {}):
    def merge_msgs(key: str, n: dict):
        def transform(doc):
            for chat in doc[key]:
                if int(chat) in n:
                    doc[key][chat] = doc[key][chat] + n[int(chat)]
        return transform
    table = up_task_db.table(category.value)
    table.update(merge_msgs('msgs', msgs), Query().id == id)


def get_tasks(category: Category):
    table = up_task_db.table(category.value)
    result_tasks = []
    for row in table.all():
        result_tasks.append(
            UpTask(row['id'],
                   row['msgs'],
                   UpTaskState(row['state']) if row['state'] else None)
        )
    return result_tasks


def remove_up_task(category: Category, id: int):
    up_task_db.table(category.value).remove(Query().id == id)


def set_attr_task(category: Category, id: int, attr: str, value):
    table = up_task_db.table(category.value)
    docs = table.search(Query().id == id)
    for task in docs:
        task['attrs'][attr] = value
    table.write_back(docs)


def get_attr_task(category: Category, id: int, attr: str):
    global up_task_db
    up_task_db = TinyDB("database/task_database.json")
    table = up_task_db.table(category.value)
    docs = table.search(Query().id == id)
    return docs[0]['attrs'][attr] if len(docs) > 0 and 'attrs' in docs[0] and attr in docs[0]['attrs'] else ''
    # if len(docs) > 0 and 'attrs' in docs[0] and attr in docs[0]['attrs']:
    #     return docs[0]['attrs'][attr]
    # else:
    #     return ''


def user_exits(user_id):
    return usr_db.contains(where('user_id') == user_id)


def add_user(user_id):
    if not user_exits(user_id):
        usr_db.insert({'user_id': user_id})


def set_category(user_id, category: Category):
    if not user_exits(user_id):
        add_user(user_id)
    usr_db.update({
        'category': category.value
    }, where('user_id') == user_id)


def get_category(user_id):
    if not user_exits(user_id):
        add_user(user_id)
    user = usr_db.search(where('user_id') == user_id)[0]
    category = None
    if 'category' in user:
        category = Category(user['category'])
    return category
