#from bs4 import BeautifulSoup
import requests
import gzip
import datetime
import urllib.parse
# import urllib.robotparser
# import re
import parse
import repository
import queue
import threading

# Инициализация репозитория


def reposytory_init():


    repository_dict = {
        'keyword': repository.DbKeywordRepository(),
        'sites': repository.DbSiteReposytory(),
        'pages': repository.DbPageRepository(),
        'person': repository.DbPersonRepository(),
        'personpagerank': repository.DbPersonPageRankRepository()
    }

    repository_worker_dict = {
        'keyword': repository.KeywordRepositoryWorker(repository_dict['keyword']),
        'sites': repository.SiteRepositoryWorker(repository_dict['sites']),
        'pages': repository.PagesRepositoryWorker(repository_dict['pages']),
        'person': repository.PersonRepositoryWorker(repository_dict['person']),
        'personpagerank': repository.PersonPageRankRepositoryWorker(repository_dict['personpagerank'])
    }
    return repository_worker_dict


class PageDowloader:
    error = None
    page = None

    def __init__(self, url):
        self.url = url

    def get_page(self):
        try:
            response = requests.get(self.url)
            if response.status_code == requests.codes.ok:
                if response.headers['Content-Type'] == 'application/octet-stream':
                    self.page = gzip.decompress(response.content)
                else:
                    self.page = response.text
            else:
                response.raise_for_status()
        except requests.exceptions.HTTPError:
            print('HTTPError!!!')
            self.error = 'httperror'
            print(self.url)
            # continue
        except requests.exceptions.ConnectionError as err:
            self.error = 'connectionerror'
            print('Connetion Error ->', err)
            print(self.url)


def findsitestorank(siteworker):
    """
    :return: Результат запроса к БД -> Список сайтов для обхода.
    """
    result = siteworker.getsitestorank()
    return result


def writerobotstodb(pagewoker, sites):
    """
    :param sites: Список сайтов для обхода краулера
    Записываем для каждого сайта в pages ссылку на robots.txt
    """
    for site in sites:
        url = urllib.parse.urlunparse(('https', site['Name'], 'robots.txt', '', '', ''))
        page = repository.Page(Url=url, SiteID=site['ID'], FoundDateTime=datetime.datetime.today())
        pagewoker.writepagestostore(page)


def pagestowalk(pagewoker):
    """
    :return: Список страниц для обхода у которых двта последнего обхода пустая
    """
    result = pagewoker.getpagelastscandatenull()
    return result


def allpages(pagewoker):
    """
    :param pagewoker:
    :return: Список всех страниц в из таблицы Pages
    """
    result = pagewoker.getallpages()
    return result


def writeurl(pagewoker, url, siteid):
    """
    :param url: Ссылка для записи в БД
    :param siteid: ID сайта для которого записываем ссылку в БД
    :return:
    """
    print('Пишем url в БД')
    page = repository.Page(Url=url, SiteID=siteid, FoundDateTime=datetime.datetime.today())
    pagewoker.writepagestostore(page)


def updatelastscandate(pagewoker, pageid):
    """
    :param pageid:
    :return:
    """
    page = repository.Page(ID=pageid, LastScanDate=datetime.datetime.today())
    pagewoker.updatepageinstore(page)


def countstatforpage(personworker, keywordworker, html):
    """
    :param html: HTML страницы которую анализируем на предмет сколько раз встречается ключевые слова.
    :return: Словаь по персонам с ID персоны и статистика для проанализируемой странице
    """
    personslist = personworker.getpersons()
    personsdict = {}
    for person in personslist:
        lst = []
        keywordslist = keywordworker.getbypersonid(person['ID'])  # GetKeywordByPersonID
        if len(keywordslist) > 0:
            for keyword in keywordslist:
                lst.append(parse.countstat(html, keyword['Name']))
            s = sum(lst)
        else:
            s = parse.countstat(html, person['Name'])
        personsdict[person['ID']] = s
    return personsdict


def writerank(personpagerankwoker, personid, pageid, rank):
    """
    :param personid:
    :param pageid:
    :param rank:
    :return:
    """
    print('Пишем Rank в БД')
    personpagerank = repository.PersonPageRank(PersonID=personid, PageID=pageid, Rank=rank)
    personpagerankwoker.writeranktostore(personpagerank)


class DbThread(threading.Thread):
    def __init__(self, repository_worker, work_queue):
        threading.Thread.__init__(self)
        self.repository_woker = repository_worker
        self.work_queue = work_queue

    def run(self):
        worker(self.repository_woker, self.work_queue)


def worker(repository_worker, pagesqueue):
    while True:
        item = pagesqueue.get()
        if item is None:
            break
        pagesset = {x['Url'] for x in allpages(repository_worker['pages'])}
        p = PageDowloader(item['Url'])
        p.get_page()
        html = p.page
        if p.error is not None:
            print(p.error)
            if p.error == 'httperror':
                print('HTTPError!!!')
                updatelastscandate(repository_worker['pages'], item['ID'])
                print(item)
                continue
            elif p.error == 'connectionerror':
                print('Connetion Error ->')
                print(item)
                continue

        if (parse.whatisurl(item['Url'])) == 'robots':
            print('Записываем ссылку на sitemap в БД')
            sitemapurl = parse.readrobots(html)
            writeurl(repository_worker['pages'], sitemapurl, item['SiteID'])
            updatelastscandate(repository_worker['pages'], item['ID'])
        elif (parse.whatisurl(item['Url'])) == 'sitemap':
            print('Получаем ссылки из sitemap и записываем в БД')
            urlstowrite = parse.sitemapparse(html)
            for url in urlstowrite:

                writeurl(repository_worker['pages'], url, item['SiteID'])
                updatelastscandate(repository_worker['pages'], item['ID'])
        else:  # Страница для анализа.

            urlsfrompage = parse.geturlfrompage(item['Url'], html)

            urlsfrompage.difference_update(pagesset)

            for url in urlsfrompage:
                writeurl(repository_worker['pages'], url, item['SiteID'])

            d = countstatforpage(repository_worker['person'], repository_worker['keyword'], html)
            for pers, rank in d.items():
                writerank(repository_worker['personpagerank'], pers, item['ID'], rank)

            updatelastscandate(repository_worker['pages'], item['ID'])

        print(item)
        pagesqueue.task_done()

def main():
    # cn = db_connect()
    # cur = cn.cursor()

    num_worker_threads = 5

    while True:
        # db = dbconnection_init()
        repository_worker = reposytory_init()
        # print('Находим сайты для обхода и записываем ссылку на robots.txt')
        sites = findsitestorank(repository_worker['sites'])
        writerobotstodb(repository_worker['pages'], sites)

        pages = pagestowalk(repository_worker['pages'])
        # print('Страниц для обхода ->', len(pages))

        pagesset = {x['Url'] for x in allpages(repository_worker['pages'])}
        # print(len(pagesset))
        # print(pagesset)
        if len(pages) > 0:
            # i = 0  # Cделал для отладки

            pagesqueue = queue.Queue()
            threads = []

            for i in range(num_worker_threads):
                repository_worker = reposytory_init()
                t = DbThread(repository_worker, pagesqueue)
                # t.daemon = True
                t.start()
                threads.append(t)

            for item in pages:
                pagesqueue.put(item)

            pagesqueue.join()

            for i in range(num_worker_threads):
                pagesqueue.put(None)
            for t in threads:
                t.join()

            '''
            for page in pages:
                pagesset = {x['Url'] for x in allpages(repository_worker['pages'])}
                p = PageDowloader(page['Url'])
                p.get_page()
                html = p.page
                if p.error is not None:
                    print(p.error)
                    if p.error == 'httperror':
                        print('HTTPError!!!')
                        updatelastscandate(repository_worker['pages'], page['ID'])
                        print(page)
                        continue
                    elif p.error == 'connectionerror':
                        print('Connetion Error ->')
                        print(page)
                        continue

                if (parse.whatisurl(page['Url'])) == 'robots':
                    print('Записываем ссылку на sitemap в БД')
                    sitemapurl = parse.readrobots(html)
                    writeurl(repository_worker['pages'], sitemapurl, page['SiteID'])
                    updatelastscandate(repository_worker['pages'], page['ID'])
                elif (parse.whatisurl(page['Url'])) == 'sitemap':
                    print('Получаем ссылки из sitemap и записываем в БД')
                    urlstowrite = parse.sitemapparse(html)
                    for url in urlstowrite:
                        # print(url)
                        writeurl(repository_worker['pages'], url, page['SiteID'])
                        updatelastscandate(repository_worker['pages'], page['ID'])
                else:  # Страница для анализа.
                    # print(page['Url'])

                    urlsfrompage = parse.geturlfrompage(page['Url'], html)

                    # print('Найденные сылки {} -> {}'.format(len(urlsfrompage), urlsfrompage))
                    urlsfrompage.difference_update(pagesset)
                    # print('Ещё не обходили ссылки ->', urlsfrompage)
                    # print('Сылок для записи -> ', len(urlsfrompage))

                    for url in urlsfrompage:
                        writeurl(repository_worker['pages'], url, page['SiteID'])

                    d = countstatforpage(repository_worker['person'], repository_worker['keyword'], html)
                    for pers, rank in d.items():
                        writerank(repository_worker['personpagerank'], pers, page['ID'], rank)

                    updatelastscandate(repository_worker['pages'], page['ID'])
                i += 1  # Cделал для отладки
                print('Осталось обойти : {} страниц из {}'.format(len(pages) - i, len(pages)))  # Cделал для отладки
                '''
        else:

            # Наброски для версии 2.0
            pages = allpages(repository_worker['pages'])
            # print(len(pages))
            t = datetime.timedelta(hours=24)
            for page in pages:
                if datetime.datetime.today() - page['LastScanDate'] > t:
                    p = urllib.parse.urlparse(page['Url'])
                    if (p.path[1:].startswith('sitemap')) and \
                            (p.path[1:].endswith('xml') or p.path[1:].endswith('xml.gz')):
                        print(p.path[1:])
                        print(datetime.datetime.today() - page['LastScanDate'])
                        pagedownload = PageDowloader(page['Url'])
                        pagedownload.get_page()
                        html = pagedownload.page
                        urlstowrite = parse.sitemapparse(html)
                        print(len(urlstowrite))
                        lst = [x['Url'] for x in pages]
                        for item in urlstowrite:
                            if item not in lst:
                                print(item)
                                writeurl(repository_worker['pages'], item, page['SiteID'])
                                updatelastscandate(repository_worker['pages'], page['ID'])
            else:
                break


if __name__ == '__main__':
    main()
