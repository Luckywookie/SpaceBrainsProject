from bs4 import BeautifulSoup
import requests
import gzip
import pymysql
import datetime

URL = 'https://lenta.ru/robots.txt'

'''
urls = [
    'https://lenta.ru/robots.txt',
    'https://gazeta.ru/robots.txt',
    'https://bfm.ru/robots.txt',
    'https://vz.ru/robots.txt',
    'https://lenta.ru/sitemap.xml',
    'https://lenta.ru/news/sitemap2.xml.gz',
    'https://gazeta.ru/sitemap.xml',
    'https://bfm.ru/sitemap.xml',
    'https://lenta.ru/news/2012/05/16/devour/',
    'https://vz.ru/sitemap.xml',
]

'''
# Списки соответсвующие таблицам
personsList = [{'ID': 1, 'Name': 'Путин'}]
keywordsList = [
    {'ID': 1, 'Name': 'Путин', 'PersonID': 1},
    {'ID': 2, 'Name': 'Путину', 'PersonID': 1},
    {'ID': 3, 'Name': 'Путина', 'PersonID': 1},
    {'ID': 4, 'Name': 'Путиным', 'PersonID': 1},
]  # Список ключевых слов
sitesList = [
    {'ID': 1, 'Name': 'lenta.ru'},
]  # Список сайтов
pagesList = []  # Список страниц
personalpagerankList = []  # Список рейтингов

'''
# Состав данных
keywordDict = {'ID': None, 'Name': None, 'PersonID': None}  #Структура ID -> int, Name -> str, PersonID -> int
sitesDict = {'ID': 1, 'Name': 'lenta.ru'}
sitesDict = {'ID': None, 'Name': None}  # Cтруктура ID -> int, Name -> str
pagesDict = {'ID': None, 'Url': None, 'SiteID': None, 'FoundDateTime': None,
             'LastScanDate': None}  # Структура ID -> int, Url -> str, Siteid -> int, FoundDateTime -> datetime, LastScanDate-> datetime
personalpagerankDict = {'ID': None, 'PageID': None,
                        'Rank': None}  # Структура PersonID -> int, PageID -> int, Rank -> int
'''


def get_html(url):
    '''
    Скачивает страницу по заданному адресу (url)
    '''
    resp = requests.get(url)
    head = resp.headers
    if resp.status_code == requests.codes.ok:
        if head['Content-Type'] == 'application/octet-stream':
            return gzip.decompress(resp.content)
        else:
            return resp.text
    else:
        return resp.status_code


def read_sites(sitesLst):
    '''
    Читает данные по сайтам (sites)
    '''
    for item in sitesLst:
        yield item

def read_keywords():
	pass


def read_pages(pageLst):  # Выдает лист для обработки
    '''
    Читаем pages и находим пустую дату последнеего сканирования 'LastScanDate': None
    '''
    lst = []
    for item in pageLst:
        if item['LastScanDate'] is None:
            lst.append(item)
    return lst


def read_pages_first(sitesLst, pagesLst):
    '''
    Формируем лист для записи в pages ссылки на robots.txt
    '''
    lst = []
    i = len(pagesLst)
    for x in sitesLst:
        # print('ID: ', i['ID'])

        if len(pagesLst) == 0:
            # print(i['Name'])
            i += 1
            url = '/'.join(['https:/', x['Name'], 'robots.txt'])
            lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                        'LastScanDate': None})
        else:
            for item in pagesLst:
                # print('SiteID: ', item['SiteID'])
                if item['SiteID'] == x:
                    continue
                else:
                    # print(i['Name'])
                    i += 1
                    url = '/'.join(['https:/', x['Name'], 'robots.txt'])
                    lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                                'LastScanDate': None})
                    print(len(pagesLst))
    return lst


def read_robots(file):
    r = file.split('\n')
    for x in r:
        if x.startswith('Sitemap'):
            return x.split(':', maxsplit=1)[-1].strip()


def write_sitemap(url, siteid, pageLst):
    i = len(pageLst)
    pageLst.append(
        {'ID': i + 1, 'Url': url, 'SiteID': siteid, 'FoundDateTime': datetime.datetime.now(), 'LastScanDate': None})


def read_html(url):
    pass


def sitemap(html):
    soup = BeautifulSoup(html, 'lxml')
    st = [url.text for url in soup.find_all('loc')]
    return st


def db_read_pages_first(sitesLst, pagesLst):
    lst = []
    i = len(pagesLst)
    for x in sitesLst:
        # print('ID: ', i['ID'])

        if len(pagesLst) == 0:
            # print(i['Name'])
            i += 1
            url = '/'.join(['https:/', x['Name'], 'robots.txt'])
            lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                        'LastScanDate': None})
        else:
            for item in pagesLst:
                # print('SiteID: ', item['SiteID'])
                if item['SiteID'] == x:
                    continue
                else:
                    # print(i['Name'])
                    i += 1
                    url = '/'.join(['https:/', x['Name'], 'robots.txt'])
                    lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                                'LastScanDate': None})
                    print(len(pagesLst))
    return lst


def db_connect():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        db='ratepersons',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    return cur


def main():
    # Проходим по таблице sites и записываем информацию pages (robots.txt)
    '''
    cur = db_connect()

    cur.execute('select * from pages')
    result = cur.fetchall()
    print(result)
    '''

    pages = read_pages_first(read_sites(sitesList), pagesList)
    pagesList.extend(pages)

    # print(read_pages(pagesList))
    p = len(read_pages(pagesList))
    print('pages : ', p)

    for i in read_pages(pagesList):
        if i['Url'].split('/')[-1].endswith('.txt'):  # Определяем куда ведет ссылка
            page = get_html(i['Url'])
            stmapurl = read_robots(page)
            write_sitemap(stmapurl, i['SiteID'], pagesList)
            i['LastScanDate'] = datetime.datetime.now()

    p = len(read_pages(pagesList))
    ask = True

    while ask:
        input('????->')
        if ask and p != 0:
            for i in read_pages(pagesList):
                print(i)
                if i['Url'].split('/')[-1].endswith('.xml') or i['Url'].split('/')[-1].endswith('.xml.gz'):
                    page = get_html(i['Url'])
                    try:
                        sitemappage = sitemap(page)
                    except TypeError:
                        i['LastScanDate'] = datetime.datetime.now()
                        print(i)
                        continue
                    print(len(sitemappage))
                    for x in sitemappage:
                        write_sitemap(x, i['SiteID'], pagesList)
                    i['LastScanDate'] = datetime.datetime.now()
                else:
                    ask = False
                print(i)
        p = len(read_pages(pagesList))
        print('pages : ', p)
        print(ask)
    print(ask) 
    p = len(read_pages(pagesList))
    print('pages : ', p)
    print(len(pagesList))



if __name__ == '__main__':
    main()
