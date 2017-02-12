from __future__ import absolute_import
import logging
import six
from scrapy.http import Request
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
import re
from scrapy.spiders import SitemapSpider
from datetime import datetime
import pymysql
from urlparse import *
from urllib2 import *

from crawler.items import BrainedItem, BrainedItemLoader
from scrapy.selector import Selector

logger = logging.getLogger(__name__)

db = pymysql.connect(host=u'93.174.131.56', port=3306, user=u'oldfox', password=u'',
                             db=u'ratepersons', charset=u'utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# db = pymysql.connect(host='localhost', port=3306, user='root', password='',
#                      db='ratepersons', charset='utf8mb4',
#                      cursorclass=pymysql.cursors.DictCursor)

cursor = db.cursor()
site_ids = {}


def get_new_sitemaps():

    sitemaps = []
    explored_sites_ids = set()
    new_sitemaps = []

    cursor.execute(u'SELECT * FROM Sites')
    for site in cursor:
        sitemap_url = urlunparse((u'https', site[u'Name'], u'/robots.txt', u'', u'', u''))
        site_ids[site[u'Name']] = site[u'ID']
        sitemaps.append(sitemap_url)

    cursor.execute(u'SELECT * FROM Pages')
    for p in cursor:
        explored_sites_ids.add(p[u'SiteID'])

    for site_name in site_ids:
        if site_ids[site_name] not in explored_sites_ids:
            robot = urlunparse((u'https', site_name, u'/robots.txt', u'', u'', u''))
            query = u'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, null)'
            cursor.execute(query, (robot, site_ids[site_name], datetime.today()))
            new_sitemaps.append(urlunparse((u'https', site_name, u'/robots.txt', u'', u'', u'')))
            explored_sites_ids.add(site_ids[site_name])
            db.commit()

    return new_sitemaps


def get_keywords():

    keywords = {}

    cursor.execute(u'select * from `Persons`')
    personslist = cursor.fetchall()
    for person in personslist:
        query = u"select * from `Keywords` where `Keywords`.`PersonID`=%s"
        cursor.execute(query, (person[u'ID'],))
        keywords[person[u'ID']] = []

    cursor.execute(u'select * from `Keywords`')
    keywordslist = cursor.fetchall()
    for keyword in keywordslist:
        if keyword[u'PersonID'] in keywords:
            keywords[keyword[u'PersonID']].append(keyword[u'Name'])

    return keywords


class GeekSitemapSpider(SitemapSpider):

    name = 'geek_sitemap_spider'

    sitemap_urls = get_new_sitemaps()
    old_sitemap_urls = []
    sitemap_follow = [u'']
    keywords = get_keywords()

    def parse(self, response):
        print u'Parsing... ', response.url
        selector = Selector(response)
        l = BrainedItemLoader(BrainedItem(), selector)
        l.add_value(u'url', response.url)

        sql = u'select * from `Pages` where `Pages`.`Url`=%s'
        cursor.execute(sql, (response.url,))
        pages = cursor.fetchall()
        try:
            page = pages[0]
        except IndexError:
            url = urlparse(response.url)
            sql = u'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
            site_id = site_ids[url.netloc]
            cursor.execute(sql, (response.url, site_id, datetime.today(), datetime.today()))
            db.commit()
            sql = u'select * from `Pages` where `Pages`.`Url`=%s'
            cursor.execute(sql, (url.geturl(),))
            pages = cursor.fetchall()
            page = pages[0]
        sql = u'update `Pages` set `LastScanDate`=%s where `Pages`.`Url`=%s'
        cursor.execute(sql, (datetime.today(), response.url))
        db.commit()

        for person in self.keywords:
            rank = 0
            print u'PersonID: ', unicode(person)
            for word in self.keywords[person]:
                print response.xpath(u'.').re(ur'\b{}\b'.format(word))
                rank += len(response.xpath(u'.').re(ur'\b{}\b'.format(word)))
            l.add_value(u'PersonID', unicode(person))
            l.add_value(u'Rank', unicode(rank))
            sql = u'insert into `personpagerank` (personid, pageid, rank) values (%s, %s, %s)'
            cursor.execute(sql, (person, page[u'ID'], rank))
            db.commit()
        print u'Parsed'
        return l.load_item()

    def start_requests(self):
        for url in self.sitemap_urls:
            yield Request(url, self._parse_sitemap)
        for url in self.old_sitemap_urls:
            yield Request(url, self._parse_oldsitemap)

    def _parse_sitemap(self, response):
        if response.url.endswith(u'/robots.txt'):
            ur = urlparse(response.url, scheme=u'https')
            sql = u'UPDATE `Pages` SET `LastScanDate`=%s WHERE `Pages`.`Url` = %s'
            cursor.execute(sql, (datetime.today(), ur.geturl()))
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                # print('sitemap_url_from_robots: ' + url)
                u = urlparse(url, scheme=u'https')
                sql = u'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
                site_id = site_ids[urlparse(url).netloc]
                cursor.execute(sql, (u.geturl(), site_id, datetime.today(), datetime.today()))
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                logger.warning(u"Ignoring invalid sitemap: %(response)s",
                               {u'response': response}, extra={u'spider': self})
                return
            s = Sitemap(body)
            if s.type == u'sitemapindex':
                for loc in iterloc(s, self.sitemap_alternate_links):
                    # print('sitemapindex.loc: ' + loc)
                    u = urlparse(loc, scheme=u'https')
                    sql = u'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
                    site_id = site_ids[urlparse(loc).netloc]
                    cursor.execute(sql, (u.geturl(), site_id, datetime.today(), datetime.today()))
                    db.commit()
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap)
            elif s.type == u'urlset':
                for loc in iterloc(s):
                    u = urlparse(loc, scheme=u'https')
                    # print('urlset.loc.https: ' + urlunparse(('https', u.netloc, u.path, '', '', '')))
                    sql = u'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, null)'
                    site_id = site_ids[urlparse(loc).netloc]
                    if u.path:
                        cursor.execute(sql, (urlunparse((u'https', u.netloc, u.path, u'', u'', u'')), site_id,
                                             datetime.today()))
                    else:
                        cursor.execute(sql, (urlunparse((u'https', u.netloc, u'/', u'', u'', u'')), site_id,
                                             datetime.today()))
                    for r, c in self._cbs:
                        if r.search(loc):
                            yield Request(loc, callback=c)
                            break
        db.commit()

    def _parse_oldsitemap(self, response):
        pass


def regex(x):
    if isinstance(x, six.string_types):
        return re.compile(x)
    return x


def iterloc(it, alt=False):
    for d in it:
        yield d[u'loc']
        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and u'alternate' in d:
            for l in d[u'alternate']:
                yield l
