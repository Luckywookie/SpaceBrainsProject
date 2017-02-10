#from bs4 import BeautifulSoup
import requests
import gzip
import datetime
import urllib.parse
# import urllib.robotparser
# import re
import parse
import repository

# Инициализация репозитория

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
    # sitesrepository = repository.DbSiteReposytory()
    # siteworker = repository.SiteRepositoryWorker(sitesrepository)
    # siteworker = repository_worker_dict['sites']
    result = siteworker.getsitestorank()
    return result


def writerobotstodb(pagewoker, sites):
    """
    :param sites: Список сайтов для обхода краулера
    Записываем для каждого сайта в pages ссылку на robots.txt
    """
    # pagesrepository = repository.DbPageRepository()
    # pagewoker = repository.PagesRepositoryWorker(pagesrepository)
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

'''
def whatisurl(url):
    """
    :param url: Ссылка для анализа, куда ведет
    :return: 'robots' или 'sitemap' в зависимости от того на что указывает ссылка.
    """
    parse = (urllib.parse.urlsplit(url))
    if parse.path.endswith('robots.txt'):
        return 'robots'
    elif parse.path.endswith('.xml') or parse.path.endswith('.xml.gz'):
        print(url)
        return 'sitemap'


def readrobots(file):
    """
    :param file: Файл robots.txt для аналза и извечения ссылки на  sitemap
    :return: Возвращает ссылку на sitemap
    """
    r = file.split('\n')
    for x in r:
        if x.startswith('Sitemap'):
            return x.split(':', maxsplit=1)[-1].strip()
'''

def writeurl(pagewoker, url, siteid):
    """
    :param url: Ссылка для записи в БД
    :param siteid: ID сайта для которого записываем ссылку в БД
    :return:
    """
    # pagesrepository = repository.DbPageRepository()
    # pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    print('Пишем url в БД')
    page = repository.Page(Url=url, SiteID=siteid, FoundDateTime=datetime.datetime.today())
    pagewoker.writepagestostore(page)


def updatelastscandate(pagewoker, pageid):
    """
    :param pageid:
    :return:
    """
    # pagesrepository = repository.DbPageRepository()
    # pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    page = repository.Page(ID=pageid, LastScanDate=datetime.datetime.today())
    pagewoker.updatepageinstore(page)

'''
def sitemapparse(html):
    """
    :param html: HTML страница sitemap для извлечения ссылок для дальнейшего обхода.
    :return: Список ссылок для записи в БД по которым необходимо совершать обход
    """
    soup = BeautifulSoup(html, 'lxml')
    st = [url.text for url in soup.find_all('loc')]
    return st


def countstat(html, word):
    """
    :param html: Страница для подсчета статистики.
    :param word: Слово по которому подсчитываем статистику
    :return: Количество раз упоминания слован на странице
    """
    soup = BeautifulSoup(html, 'lxml')
    c = r'\b{}\b'.format(word)
    w = re.compile(c)
    i = 0
    for string in soup.stripped_strings:
        if len(w.findall(repr(string))) > 0:
            i += len(w.findall(repr(string)))
    print('Rank ->', i)
    return i
'''

def countstatforpage(personworker, keywordworker, html):
    """
    :param html: HTML страницы которую анализируем на предмет сколько раз встречается ключевые слова.
    :return: Словаь по персонам с ID персоны и статистика для проанализируемой странице
    """
    # personrepository = repository.DbPersonRepository()
    # personworker = repository.PersonRepositoryWorker(personrepository)
    # keywordrepository = repository.DbKeywordRepository()
    # keywordworker = repository.KeywordRepositoryWorker(keywordrepository)
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
    # personpagerankrepository = repository.DbPersonPageRankRepository()
    # personpagerankwoker = repository.PersonPageRankRepositoryWorker(personpagerankrepository)

    print('Пишем Rank в БД')
    personpagerank = repository.PersonPageRank(PersonID=personid, PageID=pageid, Rank=rank)
    personpagerankwoker.writeranktostore(personpagerank)

'''
def geturlfrompage(url, html):
    """
    Извлекает ссылки со страницы в соответстви с правилами в robots.txt
    :param url:
    :param html:
    :return:
    """
    soup = BeautifulSoup(html, 'lxml')
    alst = soup.select('a[href^="/"]')
    p = urllib.parse.urlparse(url)
    r = urllib.robotparser.RobotFileParser()
    rurl = urllib.parse.urlunparse((p.scheme, p.netloc, 'robots.txt', '', '', ''))
    r.set_url(rurl)
    r.read()
    # print(p.netloc)
    hrefs = set()
    for link in alst:
        path = link['href'].split('?')[0]
        u = urllib.parse.urljoin(url, path)
        u1 = urllib.parse.urlparse(u)
        if p.netloc == u1.netloc:
            if r.can_fetch("*", u):
                hrefs.add(u)
    return hrefs
'''

def main():
    # cn = db_connect()
    # cur = cn.cursor()

    while True:
        # print('Находим сайты для обхода и записываем ссылку на robots.txt')
        sites = findsitestorank(repository_worker_dict['sites'])
        writerobotstodb(repository_worker_dict['pages'], sites)

        pages = pagestowalk(repository_worker_dict['pages'])
        # print('Страниц для обхода ->', len(pages))

        pagesset = {x['Url'] for x in allpages(repository_worker_dict['pages'])}
        # print(len(pagesset))
        # print(pagesset)
        if len(pages) > 0:
            i = 0  # Cделал для отладки
            for page in pages:
                pagesset = {x['Url'] for x in allpages(repository_worker_dict['pages'])}
                p = PageDowloader(page['Url'])
                p.get_page()
                html = p.page
                if p.error is not None:
                    print(p.error)
                    if p.error == 'httperror':
                        print('HTTPError!!!')
                        updatelastscandate(repository_worker_dict['pages'], page['ID'])
                        print(page)
                        continue
                    elif p.error == 'connectionerror':
                        print('Connetion Error ->')
                        print(page)
                        continue

                if (parse.whatisurl(page['Url'])) == 'robots':
                    print('Записываем ссылку на sitemap в БД')
                    sitemapurl = parse.readrobots(html)
                    writeurl(repository_worker_dict['pages'], sitemapurl, page['SiteID'])
                    updatelastscandate(repository_worker_dict['pages'], page['ID'])
                elif (parse.whatisurl(page['Url'])) == 'sitemap':
                    print('Получаем ссылки из sitemap и записываем в БД')
                    urlstowrite = parse.sitemapparse(html)
                    for url in urlstowrite:
                        # print(url)
                        writeurl(repository_worker_dict['pages'], url, page['SiteID'])
                        updatelastscandate(repository_worker_dict['pages'], page['ID'])
                else:  # Страница для анализа.
                    # print(page['Url'])

                    urlsfrompage = parse.geturlfrompage(page['Url'], html)

                    # print('Найденные сылки {} -> {}'.format(len(urlsfrompage), urlsfrompage))
                    urlsfrompage.difference_update(pagesset)
                    # print('Ещё не обходили ссылки ->', urlsfrompage)
                    # print('Сылок для записи -> ', len(urlsfrompage))

                    for url in urlsfrompage:
                        writeurl(repository_worker_dict['pages'], url, page['SiteID'])

                    d = countstatforpage(repository_worker_dict['person'], repository_worker_dict['keyword'], html)
                    for pers, rank in d.items():
                        writerank(repository_worker_dict['personpagerank'], pers, page['ID'], rank)

                    updatelastscandate(repository_worker_dict['pages'], page['ID'])
                i += 1  # Cделал для отладки
                print('Осталось обойти : {} страниц из {}'.format(len(pages) - i, len(pages)))  # Cделал для отладки
        else:

            # Наброски для версии 2.0
            pages = allpages(repository_worker_dict['pages'])
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
                                writeurl(repository_worker_dict['pages'], item, page['SiteID'])
                                updatelastscandate(repository_worker_dict['pages'], page['ID'])
            else:
                break

    repository.conn.close()


if __name__ == '__main__':
    main()
