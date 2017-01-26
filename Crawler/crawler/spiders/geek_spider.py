# from scrapy.spiders import CrawlSpider, Rule, SitemapSpider
# from scrapy.linkextractors import LinkExtractor
# from scrapy.selector import Selector
#
# from crawler.items import BrainItemLoader, BrainItem

from scrapy.spiders import SitemapSpider
from datetime import datetime
import pymysql
from urllib.parse import *

db_setup = pymysql.connect(host='localhost', user='root', password='',
                           db='ratepersons', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
sql_command = db_setup.cursor()

sitemaps = []
site_ids = {}
explored_sites = set()

sql_command.execute('SELECT * FROM Sites')
for site in sql_command:
    print('Site: {}\nSite ID: {}'.format(site['Name'], site['ID']))
    sitemap_url = urlunparse(('https', site['Name'], '/sitemap.xml', '', '', ''))
    site_ids[site['Name']] = site['ID']
    sitemaps.append(sitemap_url)
    print('Sitemap URL: ' + sitemap_url)

print(site_ids)
print(sitemaps)

sql_command.execute('SELECT * FROM Pages')
for page in sql_command:
    explored_sites.add(page['SiteID'])

print(explored_sites)

for site_name in site_ids:
    if site_ids[site_name] not in explored_sites:
        robot = urlunparse(('https', site_name, '/robots.txt', '', '', ''))
        query = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, null)'
        sql_command.execute(query, (robot, site_ids[site_name], datetime.today().strftime("%d/%m/%y %H:%M")))
        explored_sites.add(site_ids[site_name])
        db_setup.commit()

sql_command.close()
db_setup.close()


class GeekSitemapSpider(SitemapSpider):
    name = 'geek_sitemap_spider'

    sitemap_urls = sitemaps
    sitemap_follow = []

    def parse(self, response):
        url = urlparse(response.url)
        site_id = site_ids[url.netloc]
        print(response.url)
        print(site_ids[url.netloc])
        db = pymysql.connect(host='localhost', user='root', password='',
                             db='ratepersons', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = db.cursor()
        sql = 'INSERT INTO Pages (Url, SiteID, LastScanDate) VALUES (%s, %s, %s)'
        cursor.execute(sql, (url.geturl(), site_id, datetime.today().strftime("%d/%m/%y %H:%M")))
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
