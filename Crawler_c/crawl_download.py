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
keywordsList = [] # Список ключевых слов
sitesList = [
	{'ID': 1, 'Name': 'lenta.ru'},
	{'ID': 2, 'Name': 'gazeta.ru'},
]  # Список сайтов
pagesList = [] # Список страниц
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
        print(item)
        yield item

def write_robots(sitesLst, pagesLst):
    lst = []
    i = len(pagesLst)
    for x in sitesLst:
        #print('ID: ', i['ID'])

        if len(pagesLst) == 0:
            #print(i['Name'])
            i += 1
            url = '/'.join(['https:/', x['Name'], 'robots.txt'])
            lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(), 'LastScanDate': None})
        else:    
            for item in pagesLst:
                print('SiteID: ', item['SiteID'])
                if item['SiteID'] == x:
                    continue
                else:
                    #print(i['Name'])
                    i += 1
                    url = '/'.join(['https:/', x['Name'], 'robots.txt'])
                    lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(), 'LastScanDate': None})
                    print(len(pagesLst))
    return lst


def read_pages(pageLst):
    '''
    Читает pages и находит пустую датупоследнеего сканирования 'LastScanDate': None
    '''
    for item in pageLst:
        if item['LastScanDate'] is None:
            if item['Url'].split('/')[-1] == 'robots.txt':
                yield item['Url']


def read_robots(file):
    r = file.split('\n')
    for x in r:
        if x.startswith('Sitemap'):
           return x.split(':', maxsplit=1)[-1].strip()


def read_html(url):
    pass


def sitemap(html):
    soup = BeautifulSoup(html, 'lxml')
    for url in soup.find_all('loc'):
        yield url

        # st = [url.text for url in soup.find_all('loc')]
        # return st


def db_write_sitemap():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        db='ratepersons',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()


def main():
    lst = write_robots(read_sites(sitesList), pagesList)
    
    pagesList.extend(lst)
    
    print(pagesList)
    
    for i in read_pages(pagesList):
        print(i)
        print(read_robots(get_html(i)))
    

if __name__ == '__main__':
    main()
