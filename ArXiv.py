import requests
from bs4 import BeautifulSoup as Bs
import datetime
from openpyxl import Workbook
from os import path, mkdir
import cloud
import sqlite3
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import mysql.connector

#"CREATE TABLE IF NOT EXISTS ArXiv(id INT PRIMARY KEY, title TEXT, abstract TEXT, MyTagsTEXT, authors TEXT);"


def get_page(page_utl: str):
    request = requests.get(page_utl)
    page = Bs(request.content, 'html.parser')
    return page


def CheckTags(tags: list, data: str):
    add = ''
    for tag in tags:
        if data.find(tag) != -1:
            add += tag
            add += ','

    return add


def check_doubles(table_name: str, column: str, data: str, searchwords: str):
    db_connection = sqlite3.connect('MWJ.db')
    cursor = db_connection.cursor()
    cursor.execute("SELECT " + column + " FROM " + table_name + " WHERE searchwords = '" + searchwords + "'")
    column = cursor.fetchall()
    for tag in column:
        for tup in tag:
            if tup == data:
                return True
    return False


def remove_commas(string):
    trans_table = {ord(','): None, ord(':'): None, ord('.'): None, ord('&'): None, ord('!'): None, ord('"'): None,
                   ord('?'): None, ord('\n'): None, ord('\t'): None, ord('@'): None,ord("'"): None,ord("’"): None}
    return string.translate(trans_table)


def canonize(text:str):
    text = remove_commas(text)
    stop_words = stopwords.words('english')
    data = [word for word in text.split() if word not in stop_words]
    return data


def lemmatize(text:list):
    wnl = WordNetLemmatizer()
    result = ''
    for i in text:
        result += wnl.lemmatize(i)
        result += ' '
    return result


def push_arxiv_db(title: str, abstract: str, tags: str, authors: str, searchwords: str, links: str):
    db_connection = mysql.connector.connect(
        host="srv48-h-st.jino.ru",
        user="j1498375",
        passwd="b5f!g9Lemsyh",
        database="j1498375_test1"
        )
    print("Connection to MySQL DB successful")
    cursor = db_connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS ArXiv(id INT PRIMARY KEY, title TEXT, abstract TEXT, MyTags TEXT, authors TEXT, searchwords TEXT, link TEXT, normalized TEXT);")
    print("1")
    #if check_doubles('ArXiv', 'title', title, searchwords) == False:
    data1 = []
    cursor.execute("SELECT id FROM ArXiv")
    id = cursor.fetchall()
    if len(id) == 0:
        data1.append(1)
    else:
        data1.append(int(id[-1][0]) + 1)
    print("2")
    data1.append(title)
    data1.append(abstract)
    data1.append(tags)
    data1.append(authors)
    data1.append(searchwords)
    data1.append(links)
    norm = ''
    data1.append(norm)
    t = tuple(data1)
    print(t)
    cursor.execute("INSERT INTO ArXiv VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", t)
    data1.clear()
    db_connection.commit()
    db_connection.close()


def arxiv_parser(link: str, searchwords: str):
    page_num = 0
    page = get_page(page_utl=link + str(page_num))
    while page:
        page = get_page(page_utl=link + str(page_num))
        radar_tags = cloud.get_column('Tags', 'tags_list')
        radar_tags.pop()
        articles_list = page.select('.arxiv-result')
        if len(articles_list) == 0:
            break
        for article in articles_list:
            try:
                title = article.find('p', class_='title is-5 mathjax').get_text()
                title = " ".join(title.split())
                authors = [author.text for author in article.select('.authors > a')]
                authors = ", ".join(authors)
                abstract = article.find('span', class_='abstract-full has-text-grey-dark mathjax').get_text()
                abstract = abstract[: -7]
                abstract = " ".join(abstract.split())
                tags = CheckTags(radar_tags, abstract)
                links = article.find('p', class_='list-title is-inline-block').find('a').get('href')
            except AttributeError:
                title = 'Нет данных'
                authors = 'Нет данных'
                abstract = 'Нет данных'
            push_arxiv_db(title, abstract, tags, authors, searchwords, links)
        page_num += 200
    else:
        print("No more pages")





arxiv_parser("https://arxiv.org/search/?query=electronic+warfare&searchtype=all&abstracts=show&order=-announced_date_first&size=200&start=", "electronic warfare")
#arxivParser("https://arxiv.org/search/?query=mmWave&searchtype=all&abstracts=show&order=-announced_date_first&size=200&start=", "mmWave")
