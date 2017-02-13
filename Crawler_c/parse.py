# from bs4 import BeautifulSoup, SoupStrainer
import urllib.parse
import urllib.robotparser
import re


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
    result = {}
    r = file.split('\n')
    for x in r:
        if x.startswith('Sitemap'):
            result['sitemap'] = x.split(':', maxsplit=1)[-1].strip()
            #return x.split(':', maxsplit=1)[-1].strip()
        elif x.startswith('Host'):
            result['root'] = x.split(':', maxsplit=1)[-1].strip()
            # return x.split(':', maxsplit=1)[-1].strip()
    #print(result)
    if result.get('sitemap'):
        return result['sitemap']
    elif result.get('root'):
        return result['root']

def sitemapparse(soup):
    """
    :param soup: HTML страница sitemap для извлечения ссылок для дальнейшего обхода.
    :return: Список ссылок для записи в БД по которым необходимо совершать обход
    """
    # soup = BeautifulSoup(html, 'lxml')
    st = [url.text for url in soup.find_all('loc')]
    return st


def countstat(soup, word):
    """
    :param soup: Страница для подсчета статистики.
    :param word: Слово по которому подсчитываем статистику
    :return: Количество раз упоминания слован на странице
    """
    # soup = BeautifulSoup(html, 'lxml')
    c = r'\b{}\b'.format(word)
    w = re.compile(c)
    i = 0
    for string in soup.stripped_strings:
        if len(w.findall(repr(string))) > 0:
            i += len(w.findall(repr(string)))
    print('Rank ->', i)
    return i


def geturlfrompage(url, soup):
    """
    Извлекает ссылки со страницы в соответстви с правилами в robots.txt
    :param url:
    :param soup:
    :return:
    """
    # only_a = SoupStrainer('a')
    # soup = BeautifulSoup(html, 'lxml', parse_only=only_a)
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


def main():
    pass


if __name__ == '__main__':
    main()
