import urllib.request
from datetime import datetime
from sys import getsizeof
from urllib.parse import urlparse

import os.path
from os import path

from bs4 import BeautifulSoup
from tinydb import TinyDB

from database import Category, Media

import argparse

argparse = argparse.ArgumentParser()
argparse.add_argument("-save_to")
args = argparse.parse_args()

arj_media_categories = {
    Category.FILMS.value: {"url": "http://film.arjlover.net/film/"},
    Category.TALES.value: {"url": "http://multiki.arjlover.net/multiki/"},
    Category.MOVIES_FOR_CHILD.value: {"url": "http://filmiki.arjlover.net/filmiki/"},
}


def main():
    if args.save_to:
        save_to = args.save_to
        if path.exists(args.save_to):
            os.rename(save_to, '{}.backup'.format(save_to))
    else:
        save_to = 'database/generated/media_database-{}.json'.format(datetime.now().strftime("%d_%m_%Y-%I%p_%M_%S"))
    database = TinyDB(save_to)
    for category in arj_media_categories:
        r = 0
        category_table = database.table(category)
        items = []
        for media in get_arj_media(category):
            items.append({"id": media.id, "name": media.name, "http_url": media.http_url})
            r = r + 1
        category_table.insert_multiple(items)
        print("сохранено {} записей в {}".format(r, category))
    print(save_to)
    return save_to


def get_arj_media(category):
    result = []
    page_url = arj_media_categories[category]["url"]
    page_domain = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(page_url))
    page = urllib.request.urlopen(page_url)
    soup = BeautifulSoup(page.read(), "html.parser")
    i = 0
    for row in soup.select("tr[class=o], tr[class=e]"):
        row_title = row.select_one("td[class=l]")
        row_http_url = row.select_one("a:contains(http)")["href"]
        result.append(Media(i, row_title.string, page_domain + row_http_url))
        i += 1
    return result


if __name__ == '__main__':
    main()
