# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# from bs4 import BeautifulSoup
# import re
from __future__ import absolute_import
from datetime import datetime
import pymysql
from urlparse import *
from urllib2 import *
from urllib import *
from time import sleep

db = pymysql.connect(host=u'localhost', port=3306, user=u'root', password=u'',
                         db=u'ratepersons', charset=u'utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()
site_ids = {}


def get_site_ids():
    cursor.execute(u'SELECT * FROM Sites')
    for site in cursor:
        site_ids[site[u'Name']] = site[u'ID']
    return site_ids


class BrainedItemPipeline(object):
    site_ids = get_site_ids()

    def __init__(self):
        self.db = pymysql.connect(host=u'localhost', port=3306, user=u'root', password=u'',
                         db=u'ratepersons', charset=u'utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.db.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()

    def process_item(self, item, spider):
        return item
        # print(item)
        # print(item['url'])
        # print(item['Rank'])
        # print(item['PersonID'])

        # sql = 'select * from `Pages` where `Pages`.`Url`=%s'
        # self.cursor.execute(sql, (item['url'],))
        # # sleep(0.1)
        # pages = self.cursor.fetchall()
        # try:
        #     page = pages[0]
        # except IndexError:
        #     url = urlparse(item['url'])
        #     sql = 'INSERT INTO Pages (Url, SiteID, FoundDateTime, LastScanDate) VALUES (%s, %s, %s, %s)'
        #     site_id = self.site_ids[url.netloc]
        #     self.cursor.execute(sql, (item['url'], site_id, datetime.today(), datetime.today()))
        #     self.db.commit()
        #     # sleep(0.5)
        #     sql = 'select * from `Pages` where `Pages`.`Url`=%s'
        #     self.cursor.execute(sql, (url.geturl(),))
        #     pages = self.cursor.fetchall()
        #     page = pages[0]
        # sql = 'update `Pages` set `LastScanDate`=%s where `Pages`.`Url`=%s'
        # self.cursor.execute(sql, (datetime.today(), item['url']))
        # self.db.commit()
        # # sleep(0.5)
        # for pid in item['PersonID']:
        #     for rnk in item['Rank']:
        #         sql = 'insert into `personpagerank` (personid, pageid, rank) values (%s, %s, %s)'
        #         self.cursor.execute(sql, (pid, page['ID'], rnk))
        #         self.db.commit()

                # sleep(0.5)
        # self.db.commit()

# class MySQLStorePipeline(object):
#     """A pipeline to store the item in a MySQL database.
#     This implementation uses Twisted's asynchronous database API.
#     """
#
#     def __init__(self, dbpool):
#         self.dbpool = dbpool
#
#     @classmethod
#     def from_settings(cls, settings):
#         dbargs = dict(
#             host=settings['MYSQL_HOST'],
#             db=settings['MYSQL_DBNAME'],
#             user=settings['MYSQL_USER'],
#             passwd=settings['MYSQL_PASSWD'],
#             charset='utf8',
#             use_unicode=True,
#         )
#         dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
#         return cls(dbpool)
#
#     def process_item(self, item, spider):
#         # run db query in the thread pool
#         d = self.dbpool.runInteraction(self._do_upsert, item, spider)
#         d.addErrback(self._handle_error, item, spider)
#         # at the end return the item in case of success or failure
#         d.addBoth(lambda _: item)
#         # return the deferred instead the item. This makes the engine to
#         # process next item (according to CONCURRENT_ITEMS setting) after this
#         # operation (deferred) has finished.
#         return d
#
#     def _do_upsert(self, conn, item, spider):
#         """Perform an insert or update."""
#         guid = self._get_guid(item)
#         now = datetime.utcnow().replace(microsecond=0).isoformat(' ')
#
#         conn.execute("""SELECT EXISTS(
#             SELECT 1 FROM website WHERE guid = %s
#         )""", (guid, ))
#         ret = conn.fetchone()[0]
#
#         if ret:
#             conn.execute("""
#                 UPDATE website
#                 SET name=%s, description=%s, url=%s, updated=%s
#                 WHERE guid=%s
#             """, (item['name'], item['description'], item['url'], now, guid))
#             spider.log("Item updated in db: %s %r" % (guid, item))
#         else:
#             conn.execute("""
#                 INSERT INTO website (guid, name, description, url, updated)
#                 VALUES (%s, %s, %s, %s, %s)
#             """, (guid, item['name'], item['description'], item['url'], now))
#             spider.log("Item stored in db: %s %r" % (guid, item))
#
#     def _handle_error(self, failure, item, spider):
#         """Handle occurred on db interaction."""
#         # do nothing, just log
#         log.err(failure)
#
#     def _get_guid(self, item):
#         """Generates an unique identifier for a given item."""
#         # hash based solely in the url field
#         return md5(item['url']).hexdigest()

# Cannot use this to create the table, must have table already created

# from twisted.enterprise import adbapi
# import datetime
# import MySQLdb.cursors
#
#
# class SQLStorePipeline(object):
#     def __init__(self):
#         self.dbpool = adbapi.ConnectionPool('MySQLdb', db='mydb',
#                                             user='myuser', passwd='mypass', cursorclass=MySQLdb.cursors.DictCursor,
#                                             charset='utf8', use_unicode=True)
#
#     def process_item(self, item, spider):
#         # run db query in thread pool
#         query = self.dbpool.runInteraction(self._conditional_insert, item)
#         query.addErrback(self.handle_error)
#
#         return item
#
#     def _conditional_insert(self, tx, item):
#         # create record if doesn't exist.
#         # all this block run on it's own thread
#         tx.execute("select * from websites where link = %s", (item['link'][0],))
#         result = tx.fetchone()
#         if result:
#             log.msg("Item already stored in db: %s" % item, level=log.DEBUG)
#         else:
#             tx.execute( \
#                 "insert into websites (link, created) "
#                 "values (%s, %s)",
#                 (item['link'][0],
#                  datetime.datetime.now())
#             )
#             log.msg("Item stored in db: %s" % item, level=log.DEBUG)
#
#     def handle_error(self, e):
#         log.err(e)
