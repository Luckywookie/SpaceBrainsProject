# from scrapy.spiders import CrawlSpider, Rule, SitemapSpider
# from scrapy.linkextractors import LinkExtractor
# from scrapy.selector import Selector
#
# from crawler.items import BrainItemLoader, BrainItem

import re
import logging
import six

from scrapy.spiders import Spider
from scrapy.http import Request, XmlResponse
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy.utils.gz import gunzip, is_gzipped

logger = logging.getLogger(__name__)

from scrapy.spiders import SitemapSpider
from datetime import datetime
import pymysql
from urllib.parse import *

sitemaps = []
site_ids = {}
explored_sites_ids = set()
new_sitemaps = []
keywords = []
persons = []
ranks = []

db = pymysql.connect(host='localhost', user='root', password='',
                             db='ratepersons', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()


def get_sites():

    cursor.execute('SELECT * FROM Sites')
    for site in cursor:
        print('Site: {}\nSite ID: {}'.format(site['Name'], site['ID']))
        sitemap_url = urlunparse(('https', site['Name'], '/robots.txt', '', '', ''))
        site_ids[site['Name']] = site['ID']
        sitemaps.append(sitemap_url)
        print('Sitemap URL: ' + sitemap_url)

    print(site_ids)
    print(sitemaps)

    cursor.execute('SELECT * FROM Pages')
    for page in cursor:
        explored_sites_ids.add(page['SiteID'])

    print(explored_sites_ids)

    for site_name in site_ids:
        if site_ids[site_name] not in explored_sites_ids:
            robot = urlunparse(('https', site_name, '/robots.txt', '', '', ''))
            query = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, null)'
            cursor.execute(query, (robot, site_ids[site_name], datetime.today().strftime("%d/%m/%y %H:%M")))
            new_sitemaps.append(urlunparse(('https', site_name, '/robots.txt', '', '', '')))
            explored_sites_ids.add(site_ids[site_name])
            db.commit()

    print(new_sitemaps)

    cursor.execute('SELECT * FROM Keywords')
    for word in cursor:
        keywords.append(word)

    print(keywords)

    cursor.execute('SELECT * FROM Persons')


class GeekSitemapSpider(SitemapSpider):
    name = 'geek_sitemap_spider'
    get_sites()

    sitemap_urls = new_sitemaps
    sitemap_follow = []
    old_sitemap_urls = ()

    def parse(self, response):
        url = urlparse(response.url)
        print('page_url_from_parse: ' + url.geturl())
        cursor = db.cursor()
        sql = 'UPDATE `Pages` SET `LastScanDate`=%s WHERE `Pages`.`Url` = %s'
        cursor.execute(sql, (datetime.today().strftime("%d/%m/%y %H:%M"), url.geturl()))
        db.commit()

        rank = {}
        for word in keywords:
            reg = '//text()[re:test(., "a-z")]'
            theHTML = response.xpath(reg).extract()
            print(theHTML)
            print(word['Name'])
            rank[word['PersonID']] = 0
            if word['Name'] in theHTML:
                rank[word['PersonID']] += 1
        for x in rank:
            if rank[x] > 0:
                print(rank)
                ranks.append([rank, response.url])
                print('RANKS')
                for r in ranks:
                    print(r)
        print('\nNEXT PAGE\n')

    # sitemap_rules = [('', 'parse')]
    # sitemap_follow = ['']
    # sitemap_alternate_links = False

    # def __init__(self, *a, **kw):
    #     super(SitemapSpider, self).__init__(*a, **kw)
    #     self._cbs = []
    #     for r, c in self.sitemap_rules:
    #         if isinstance(c, six.string_types):
    #             c = getattr(self, c)
    #         self._cbs.append((regex(r), c))
    #     self._follow = [regex(x) for x in self.sitemap_follow]

    def start_requests(self):
        for url in self.sitemap_urls:
            yield Request(url, self._parse_sitemap)
        for url in self.old_sitemap_urls:
            yield Request(url, self._parse_oldsitemap)

    def _parse_sitemap(self, response):
        cursor = db.cursor()
        if response.url.endswith('/robots.txt'):
            ur = urlparse(response.url, scheme='https')
            sql = 'UPDATE `Pages` SET `LastScanDate`=%s WHERE `Pages`.`Url` = %s'
            cursor.execute(sql, (datetime.today().strftime("%d/%m/%y %H:%M"), ur.geturl()))
            db.commit()
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                print('sitemap_url_from_robots: ' + url)
                u = urlparse(url, scheme='https')
                sql = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
                site_id = site_ids[urlparse(url).netloc]
                cursor.execute(sql, (u.geturl(), site_id, datetime.today().strftime("%d/%m/%y %H:%M"), datetime.today().strftime("%d/%m/%y %H:%M")))
                db.commit()
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                logger.warning("Ignoring invalid sitemap: %(response)s",
                               {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body)
            if s.type == 'sitemapindex':
                for loc in iterloc(s, self.sitemap_alternate_links):
                    print('sitemapindex.loc: ' + loc)
                    u = urlparse(loc, scheme='https')
                    sql = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
                    site_id = site_ids[urlparse(loc).netloc]
                    cursor.execute(sql, (u.geturl(), site_id, datetime.today().strftime("%d/%m/%y %H:%M"), datetime.today().strftime("%d/%m/%y %H:%M")))
                    db.commit()
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap)
            elif s.type == 'urlset':
                for loc in iterloc(s):
                    print('urlset.loc: ' + loc)
                    u = urlparse(loc, scheme='https')
                    print('https: ' + urlunparse(('https', u.netloc, u.path, '', '', '')))
                    sql = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, null)'
                    site_id = site_ids[urlparse(loc).netloc]
                    if u.path:
                        cursor.execute(sql, (urlunparse(('https', u.netloc, u.path, '', '', '')), site_id,
                                             datetime.today().strftime("%d/%m/%y %H:%M")))
                    else:
                        cursor.execute(sql, (urlunparse(('https', u.netloc, '/', '', '', '')), site_id,
                                             datetime.today().strftime("%d/%m/%y %H:%M")))
                    db.commit()
                    for r, c in self._cbs:
                        if r.search(loc):
                            yield Request(loc, callback=c)
                            break

    def _parse_oldsitemap(self, response):
        pass

    def _get_sitemap_body(self, response):
        """Return the sitemap body contained in the given response,
        or None if the response is not a sitemap.
        """
        if isinstance(response, XmlResponse):
            return response.body
        elif is_gzipped(response):
            return gunzip(response.body)
        elif response.url.endswith('.xml'):
            return response.body
        elif response.url.endswith('.xml.gz'):
            return gunzip(response.body)


def regex(x):
    if isinstance(x, six.string_types):
        return re.compile(x)
    return x


def iterloc(it, alt=False):
    for d in it:
        yield d['loc']

        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and 'alternate' in d:
            for l in d['alternate']:
                yield l

# class GeekSpider(CrawlSpider):
#     name = 'geek_spider'
#
#     start_urls = ['https://geekbrains.ru/courses']
#     allowed_domains = ['geekbrains.ru']
#
#     rules = (
#         Rule(
#             LinkExtractor(
#                 restrict_xpaths=['//div[@class="searchable-container"]'],
#                 allow=r'https://geekbrains.ru/\w+/\d+$'
#             ),
#             callback='parse_item'
#         ),
#     )
#
#     def parse_item(self, response):
#         selector = Selector(response)
#         l = BrainItemLoader(BrainItem(), selector)
#         l.add_value('url', response.url)
#         l.add_xpath('title', '//div[@class="div padder-v m-r m-l"]'
#                              '/h1/text()')
#         l.add_xpath('subtitle', '//div[@class="div padder-v m-r m-l"]'
#                                 '/div[@class="h2"]/text()')
#         l.add_xpath('logo', '//div[@class="text-center m-b-lg"]'
#                             '/img/@src')
#         l.add_xpath('description', '//div[@class="m-b-xs m-t m-r-xl"]'
#                                    '/p/text()')
#         l.add_xpath('teachers', '//div[@id="authors"]'
#                                 '//p[@class="m-l m-b-xs m-t-xs m-r-xs"]'
#                                 '/text()')
#         return l.load_item()

