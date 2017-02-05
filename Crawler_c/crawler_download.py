from bs4 import BeautifulSoup
import requests
import gzip
import datetime
import urllib.parse
import re
import repository


def get_page(url):
    """
    :param url: Ссылка на скачиваемый ресурс/страницу
    :return: HTML странца скаченная по ссылке
    """
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        if response.headers['Content-Type'] == 'application/octet-stream':
            return gzip.decompress(response.content)
        else:
            return response.text
    else:
        response.raise_for_status()


def findsitestorank():
    """
    :return: Результат запроса к БД -> Список сайтов для обхода.
    """
    sitesrepository = repository.DbSiteReposytory()
    siteworker = repository.SiteRepositoryWorker(sitesrepository)
    result = siteworker.getsitestorank()
    return result


def writerobotstodb(sites):
    """
    :param sites: Список сайтов для обхода краулера
    Записываем для каждого сайта в pages ссылку на robots.txt
    """
    pagesrepository = repository.DbPageRepository()
    pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    for site in sites:
        url = urllib.parse.urlunparse(('https', site['Name'], 'robots.txt', '', '', ''))
        page = repository.Page(Url=url, SiteID=site['ID'], FoundDateTime=datetime.datetime.today())
        pagewoker.writepagestostore(page)


def pagestowalk():
    """
    :return: Список страниц для обхода у которых двта последнего обхода пустая
    """
    pagesrepository = repository.DbPageRepository()
    pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    result = pagewoker.getpagelastscandatenull()
    return result


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


def writeurl(url, siteid):
    """
    :param url: Ссылка для записи в БД
    :param siteid: ID сайта для которого записываем ссылку в БД
    :return:
    """
    pagesrepository = repository.DbPageRepository()
    pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    print('Пишем url в БД')
    page = repository.Page(Url=url, SiteID=siteid, FoundDateTime=datetime.datetime.today())
    pagewoker.writepagestostore(page)


def updatelastscandate(pageid):
    """
    :param pageid:
    :return:
    """
    pagesrepository = repository.DbPageRepository()
    pagewoker = repository.PagesRepositoryWorker(pagesrepository)
    page = repository.Page(ID=pageid, LastScanDate=datetime.datetime.today())
    pagewoker.updatepageinstore(page)


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


def countstatforpage(html):
    """
    :param html: HTML страницы которую анализируем на предмет сколько раз встречается ключевые слова.
    :return: Словаь по персонам с ID персоны и статистика для проанализируемой странице
    """
    personrepository = repository.DbPersonRepository()
    personworker = repository.PersonRepositoryWorker(personrepository)
    keywordrepository = repository.DbKeywordRepository()
    keywordworker = repository.KeywordRepositoryWorker(keywordrepository)
    personslist = personworker.getpersons()
    personsdict = {}
    for person in personslist:
        lst = []
        keywordslist = keywordworker.getbypersonid(person['ID'])  # GetKeywordByPersonID
        for keyword in keywordslist:
            lst.append(countstat(html, keyword['Name']))
        s = sum(lst)
        personsdict[person['ID']] = s
    return personsdict


def writerank(personid, pageid, rank):
    """
    :param personid:
    :param pageid:
    :param rank:
    :return:
    """
    personpagerankrepository = repository.DbPersonPageRankRepository()
    personpagerankwoker = repository.PersonPageRankRepositoryWorker(personpagerankrepository)

    print('Пишем Rank в БД')
    personpagerank = repository.PersonPageRank(PersonID=personid, PageID=pageid, Rank=rank)
    personpagerankwoker.writeranktostore(personpagerank)


def main():
    # cn = db_connect()
    # cur = cn.cursor()

    while True:
        print('Находим сайты для обхода и записываем ссылку на robots.txt')
        sites = findsitestorank()
        writerobotstodb(sites)

        pages = pagestowalk()
        print('Страниц для обхода ->', len(pages))

        if len(pages) > 0:
            i = 0  # Cделал для отладки
            for page in pages:
                try:
                    html = get_page(page['Url'])
                except requests.exceptions.HTTPError:
                    print('HTTPError!!!')
                    updatelastscandate(page['ID'])
                    print(page)
                    continue
                except requests.exceptions.ConnectionError as err:
                    print('Connetion Error ->', err)
                    print(page)
                    continue

                if (whatisurl(page['Url'])) == 'robots':
                    print('Записываем ссылку на sitemap в БД')
                    sitemapurl = readrobots(html)
                    writeurl(sitemapurl, page['SiteID'])
                    updatelastscandate(page['ID'])
                elif (whatisurl(page['Url'])) == 'sitemap':
                    print('Получаем ссылки из sitemap и записываем в БД')
                    urlstowrite = sitemapparse(html)
                    for url in urlstowrite:
                        print(url)
                        writeurl(url, page['SiteID'])
                        updatelastscandate(page['ID'])
                else:  # Страница для анализа.
                    print(page['Url'])
                    d = countstatforpage(html)
                    for pers, rank in d.items():
                        writerank(pers, page['ID'], rank)
                    updatelastscandate(page['ID'])
                i += 1  # Cделал для отладки
                print('Осталось обойти : {} страниц из {}'.format(len(pages) - i, len(pages)))  # Cделал для отладки
        else:
            break
    repository.conn.close()


if __name__ == '__main__':
    main()
