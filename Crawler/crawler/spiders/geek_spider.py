# from scrapy.spiders import CrawlSpider, Rule, SitemapSpider
# from scrapy.linkextractors import LinkExtractor
# from scrapy.selector import Selector
#
# from crawler.items import BrainItemLoader, BrainItem

from scrapy.spiders import SitemapSpider

import pymysql

from urllib.parse import *

db = pymysql.connect(host='localhost', user='root', password='',
                     db='ratepersons', charset='utf8mb4',
                     cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()

sitemaps = []
site_ids = {}
explored_sites = set()

cursor.execute('SELECT * FROM Sites')

for row in cursor:
    print('Site: {}\nSite ID: {}'.format(row['Name'], row['ID']))
    sitemap_url = urlunparse(('https', row['Name'], '/sitemap.xml', '', '', ''))
    site_ids[row['Name']] = row['ID']
    sitemaps.append(sitemap_url)
    print('Sitemap URL: ' + sitemap_url)

print(site_ids)
print(sitemaps)

cursor.execute('SELECT * FROM Pages')

for row in cursor:
    explored_sites.add(row['SiteID'])

print(explored_sites)

for site in site_ids:
    if site_ids[site] not in explored_sites:
        robot = urlunparse(('https', site, '/robots.txt', '', '', ''))
        sql = 'INSERT INTO Pages (Url, SiteID, LastScanDate) VALUES (%s, %s, null)'
        cursor.execute(sql, (robot, site_ids[site]))
        explored_sites.add(site_ids[site])
        db.commit()

cursor.close()
db.close()


class GeekSitemapSpider(SitemapSpider):
    name = 'geek_sitemap_spider'

    sitemap_urls = sitemaps

    def parse(self, response):
        url = urlparse(response.url, scheme='https')
        site_id = site_ids[url.netloc]
        print(response.url)
        print(site_ids[url.netloc])
        db = pymysql.connect(host='localhost', user='root', password='',
                             db='ratepersons', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = db.cursor()
        sql = 'INSERT INTO Pages (Url, SiteID) VALUES (%s, %s)'
        cursor.execute(sql, (url.path, site_id))
        db.commit()
        db.close()


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
