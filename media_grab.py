import sys
import time
import urllib.request
from enum import Enum
from urllib.parse import urlparse

import psycopg2
from bs4 import BeautifulSoup
from psycopg2._psycopg import Error
from psycopg2.extras import DictCursor, execute_values

from database import Media
from properties import *

try:
    connect = psycopg2.connect(
        host=DATABASE_HOST,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        dbname=DATABASE_NAME
    )
    cursor = connect.cursor(cursor_factory=DictCursor)
    print('connected to database')
except Error as err:
    print(err.pgerror)
    print(err.diag.message_detail)
    sys.exit(1)


class ArjMediaCatLink(Enum):
    FILMS = "http://film.arjlover.net/film/"
    TALES = "http://multiki.arjlover.net/multiki/"
    SMALL_FILMS = "http://filmiki.arjlover.net/filmiki/"


def main():
    for category in (ArjMediaCatLink):
        category_start_time = time.time()
        execute_values(cursor, 'INSERT INTO media (name, url) VALUES %s',
                       [(m.name, m.http_url) for m in get_arj_media(category)]
                       )
        print('scanning {} finished {}'.format(category, time.time() - category_start_time))
    connect.commit()
    print('inserted {} items to database')


def get_arj_media(category: ArjMediaCatLink):
    result = []
    page_url = category.value
    page_domain = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(page_url))
    page = urllib.request.urlopen(page_url)
    soup = BeautifulSoup(page.read(), "html.parser")
    i = 0
    for row in soup.select("tr[class=o], tr[class=e]"):
        row_title = row.select_one("td[class=l]")
        row_http_url = row.select_one("a:contains(http)")["href"]
        result.append(Media(i, row_title.string, page_domain + row_http_url))
        i += 1
    print('{} media parsed'.format(i))
    return result


if __name__ == '__main__':
    main()
