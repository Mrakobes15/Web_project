import requests
from bs4 import BeautifulSoup as Bs
import datetime
from openpyxl import Workbook
from os import path, mkdir
import numpy as np
import matplotlib.pyplot as plt

import re
"""import cloud
import main
import my_tags
import sqlite3
"""

def check_doubles_mwj(table_name: str, column: str, title: str):
    db_connection = sqlite3.connect('MWJ.db')
    cursor = db_connection.cursor()
    cursor.execute("SELECT " + column + " FROM " + table_name + " WHERE title = '" + title + "'")
    column = cursor.fetchall()
    if len(column) > 0:
        return True
    return False


def push_mwj_db(title: str, abstract: str, tags: str, date: datetime, link: str, table_name: str):
    db_connection = sqlite3.connect('MWJ.db')
    cursor = db_connection.cursor()
    if  check_doubles_mwj(table_name, 'title', title) == False:
        data1 = []
        cursor.execute("SELECT id FROM " + table_name)
        id = cursor.fetchall()
        if len(id) == 0:
            data1.append(1)
        else:
            data1.append(int(id[-1][0]) + 1)
        data1.append(date)
        data1.append(tags)
        data1.append(title)
        data1.append(abstract)
        data1.append(link)
        data1.append(' ')
        t = tuple(data1)
        cursor.execute("INSERT INTO " + table_name + " VALUES(?, ?, ?, ?, ?, ?, ?);", t)
        data1.clear()
        db_connection.commit()
    db_connection.close()


def mwj_parser(link: str, table_name: str):
    db_connection = sqlite3.connect('MWJ.db')
    cursor = db_connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + " (id INT PRIMARY KEY, date TEXT, mwj_tags TEXT, title TEXT, abstract TEXT, link TEXT, my_tags TEXT)")
    months_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                   'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}
    page_num = 14
    page = main.get_page(page_utl=link + str(page_num))
    radar_tags = cloud.get_column('Tags', 'RadarTags')
    radar_tags.pop()
    new_articles = 0
    while page:
        page = main.get_page(page_utl=link + str(page_num))
        articles_list = page.select('.article-summary__details')
        if len(articles_list) == 0:
            break
        for article in articles_list:
            try:
                title = article.find('h2', class_='headline article-summary__headline').get_text()
                title = " ".join(title.split())
                title = re.sub("['@]", '', title)
                links = article.find('h2', class_='headline article-summary__headline').find('a').get('href')
                info = article.find('div', class_='date article-summary__post-date').get_text()
                month, day, year = info.split()
                info = '{day}.{month}.{year}'.format(
                    day=day[:-1], month=months_dict[month], year=year,
                )
                date = datetime.datetime.strptime(info, '%d.%m.%Y').date()
                tag_list = [tag.text for tag in main.get_page(links).select('.tags > a')]
                tags = ''
                for i in tag_list:
                    tags += i
                    tags += ', '
                tags = tags[: -2]
                abstract = article.find('div', class_='abstract article-summary__teaser').get_text()
                abstract = " ".join(abstract.split())
            except AttributeError:
                title = 'Нет данных'
                tags = 'Нет данных'
                abstract = 'Нет данных'
                date = datetime.date.today()
            if check_doubles_mwj(table_name, 'title', title) == False:
                push_mwj_db(title, abstract, tags, date, links, table_name)
                new_articles += 1
            else:
                return
        page_num += 1
    print("New articles:", end=" ")
    print(new_articles)
    my_tags.RefreshMyTags(table_name)


#mwj_parser("http://www.microwavejournal.com/articles/topic/3886?page=", "AdvancedCellular")
#mwj_parser("https://www.microwavejournal.com/articles/topic/3572?page=", "Microelectronics")
#mwj_parser("https://www.microwavejournal.com/articles/topic/3372?page=", "AerospaceDefense")
#mwj_parser("https://www.microwavejournal.com/articles/topic/3797?page=", "Broadband")
#mwj_parser("https://www.microwavejournal.com/articles/topic/3794?page=", "EMI")
