from bs4 import BeautifulSoup
import requests
import gzip
import pymysql
import datetime
#import sqlite3

# Списки соответсвующие таблицам
personsList = [
    {'ID': 1, 'Name': 'Путин'},
    {'ID': 2, 'Name': 'Медведев'},
]

keywordsList = [
    {'ID': 1, 'Name': 'Путин', 'PersonID': 1},
    {'ID': 2, 'Name': 'Путину', 'PersonID': 1},
    {'ID': 3, 'Name': 'Путина', 'PersonID': 1},
    {'ID': 4, 'Name': 'Путиным', 'PersonID': 1},
    {'ID': 5, 'Name': 'Медведев', 'PersonID': 2},
]  # Список ключевых слов
sitesList = [
    {'ID': 1, 'Name': 'geekbrains.ru'},
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
personalpagerankDict = {'PersonID': None, 'PageID': None,
                        'Rank': None}  # Структура PersonID -> int, PageID -> int, Rank -> int
'''


def get_html(url):
    '''
    Скачивает страницу по заданному адресу (url)
    '''
    resp = requests.get(url)
    #print(resp.encoding)
    head = resp.headers
    if resp.status_code == requests.codes.ok:
        if head['Content-Type'] == 'application/octet-stream':
            return gzip.decompress(resp.content)
        else:
            #print(resp.text)
            return resp.text
    else:
        return resp.status_code


def read_sites(sitesLst):
    '''
    Читает данные по сайтам (sites)
    '''
    for item in sitesLst:
        return item


def read_person(personsLst):
    '''
    Читаем персону из списка персон
    '''
    for item in personsLst:
        yield item


def db_read_person(cur):
    '''
    Читаем персону из списка персон
    '''
    sql = "select * from `Persons`"
    cur.execute(sql)
    result = cur.fetchall()
    return result



def read_keywords(person):
    '''
    Формируем списко ключевых слов для поиску по персоне.
    '''
    lst = []
    for keyword in keywordsList:
        if keyword['PersonID'] == person['ID']:
            lst.append(keyword['Name'])
    return lst


def db_read_keywords(cur, person):
    '''
    Формируем списко ключевых слов для поиску по персоне.
    '''
    sql = "select * from `Keywords` where `Keywords`.`PersonID`=%s"
    cur.execute(sql, (person['ID'], ))
    result = cur.fetchall()
    return result


def read_pages(pageLst):  # Выдает лист для обработки
    '''
    Читаем pages и находим пустую дату последнеего сканирования 'LastScanDate': None
    '''
    lst = []
    for item in pageLst:
        if item['LastScanDate'] is None:
            lst.append(item)
    return lst


def db_read_pages(cur):  # Выдает лист для обработки
    '''
    Читаем pages и находим пустую дату последнеего сканирования 'LastScanDate': None
    '''
    sql = "select * from `Pages` where `Pages`.`LastScanDate` is null"
    cur.execute(sql)
    result = cur.fetchall()
    return result


def read_pages_first(sitesLst, pagesLst):
    '''
    Формируем лист для записи в pages ссылки на robots.txt
    '''
    lst = []
    i = len(pagesLst)
    if len(pagesLst) == 0:
        for x in sitesLst:
            i += 1
            url = '/'.join(['https:/', x['Name'], 'robots.txt'])
            lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                        'LastScanDate': None})
    else:
        for x in sitesLst:
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


def db_write_sitemap(cur, url, siteid):
    sql = "insert into `Pages` (Url, SiteID, FoundDateTime) values (%s, %s, %s) "
    cur.execute(sql, (url, siteid, datetime.datetime.now()))


def read_html_to_stat(page):
    '''
    Читает страницу и просматривет ее на наличие ключевых слов
    '''
    soup = BeautifulSoup(page, 'lxml')
    #print(soup.get_text())
    text = soup.get_text()


    return text


def sitemap(html):
    soup = BeautifulSoup(html, 'lxml')
    st = [url.text for url in soup.find_all('loc')]
    return st


def db_read_pages_first(sitesLst, pagesLst):
    lst = []
    i = len(pagesLst)
    if len(pagesLst) == 0:
        for x in sitesLst:
            i += 1
            url = '/'.join(['https:/', x['Name'], 'robots.txt'])
            lst.append({'ID': i, 'Url': url, 'SiteID': x['ID'], 'FoundDateTime': datetime.datetime.now(),
                        'LastScanDate': None})
    else:
        for x in sitesLst:
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
    '''
    conn = sqlite3.connect('ratepersons.db3')
    cur = conn.cursor()
    '''
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        db='ratepersons',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    return conn


def main():
    # Проходим по таблице sites и записываем информацию pages (robots.txt)

    cn = db_connect()
    cur = cn.cursor()

    '''
    cur.execute('select * from Sites')
    sitesListDB = cur.fetchall()
    cur.execute('select * from Pages')
    pagesListDB = cur.fetchall()
    #print(sitesListDB)
    #print(pagesListDB)

    pages = db_read_pages_first(sitesListDB, pagesListDB)
    print(pages)

    sql = "insert into `Pages` (Url, SiteID, FoundDateTime) values (%s, %s, %s) "
    for item in pages:
        cur.execute(sql, (item['Url'], item['SiteID'], item['FoundDateTime']))
    cn.commit()


    # print(read_pages(pagesList))
    #p = len(read_pages(pagesList))
    #print('pages : ', p)

    cur.execute('select * from Pages')
    pagesListDB = cur.fetchall()
    print('pages : ', len(pagesListDB))

    #Находим sitemap и хаписываем ссылку в pagesList
    print('Записываем sitemap в PAGES')
    '''
    for i in read_pages(pagesList):
        if i['Url'].split('/')[-1].endswith('.txt'):  # Определяем куда ведет ссылка
            page = get_html(i['Url'])
            stmapurl = read_robots(page)
            write_sitemap(stmapurl, i['SiteID'], pagesList)
            i['LastScanDate'] = datetime.datetime.now()
    '''

    p = db_read_pages(cur)
    for item in p:
        print(item)
        if item['Url'].split('/')[-1].endswith('.txt'):  # Определяем куда ведет ссылка
            page = get_html(item['Url'])
            stmapurl = read_robots(page)
            db_write_sitemap(cur, stmapurl, item['SiteID'])
            sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
            t = (datetime.datetime.now(), item['ID'])
            cur.execute(sql, t)
    cn.commit()

    cur.execute('select * from Pages')
    pagesListDB = cur.fetchall()
    print('pages : ', len(pagesListDB))

    i = 0
    ask = True
    print('Читаем sitemap и записываем ссылки в pages')
    #Находми ссылки на странички
    while ask:
        input('????->')
        i +=1
        p = db_read_pages(cur)
        print(len(p), 'C -> ', i)
        if ask and len(p) != 0:
            for item in p:
                print(item)
                if item['Url'].split('/')[-1].endswith('.xml') or item['Url'].split('/')[-1].endswith('.xml.gz'):
                    page = get_html(item['Url'])
                    try:
                        sitemappage = sitemap(page)
                    except TypeError:
                        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
                        t = (datetime.datetime.now(), item['ID'])
                        cur.execute(sql, t)
                        print(item)
                        continue
                    print('sitemap -> ', len(sitemappage))
                    for x in sitemappage:
                        db_write_sitemap(cur, x, item['SiteID'])
                        sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
                        t = (datetime.datetime.now(), item['ID'])
                        cur.execute(sql, t)
                else:
                    ask = False
    cn.commit()


    cur.execute('select * from Pages')
    pagesListDB = cur.fetchall()
    print('pages : ', len(pagesListDB))


    p = len(read_pages(pagesList))
    ask = True
    print('Читаем sitemap и записываем ссылки в pages')
    #Находми ссылки на странички
    while ask:
        #input('????->')
        if ask and p != 0:
            for i in read_pages(pagesList):
                #print(i)
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
    '''

    #Проходим по полученным ссылкам и подсчитываем статитстику

    pages = db_read_pages(cur)
    print('Считаем статитстику')
    print(len(pages))
    for p in pages:
        page = read_html_to_stat(get_html(p['Url']))
        # print(page)
        for item in db_read_person(cur):
            lst = []
            print('person ->', item)
            kw = db_read_keywords(cur, item)
            print('kw ->', kw)
            for k in kw:
                lst.append((page.count(k['Name']), k['Name']))
            print('Page -> ', p['Url'])
            print(lst)
            s = sum([x[0] for x in lst])
            print('rank ->', s)

            sql = 'insert into `personpagerank` (personid, pageid, rank) values (%s, %s, %s)'
            t = (item['ID'], p['ID'], s)
            cur.execute(sql, t)


            #personalpagerankList.append({'PersonID': item['ID'], 'PageID': p['ID'], 'Rank': s})

            sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`ID` = %s'
            t = (datetime.datetime.now(), p['ID'])
            cur.execute(sql, t)
        cn.commit()

            #p['LastScanDate'] = datetime.datetime.now()
    cn.commit()
    cn.close()
    #print(personalpagerankList)


    '''
    pages = read_pages(pagesList)
    print('Считаем статитстику')
    print(len(pages))
    for p in pages:
        page = read_html_to_stat(get_html(p['Url']))
        #print(page)
        for item in read_person(personsList):
            lst = []
            print(item)
            kw = read_keywords(item)
            print(kw)
            for k in kw:
                lst.append((page.count(k), k))
            print(lst)
            s = sum([x[0] for x in lst])
            print(s)
            personalpagerankList.append({'PersonID': item['ID'], 'PageID': p['ID'], 'Rank': s})
            p['LastScanDate'] = datetime.datetime.now()

    print(personalpagerankList)
    '''

if __name__ == '__main__':
    main()
