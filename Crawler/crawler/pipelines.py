# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs

# import sys
# import MySQLdb
# import hashlib
# from scrapy.exceptions import DropItem
# from scrapy.http import Request
#
# from datetime import datetime
# from hashlib import md5
# from scrapy import log
# from scrapy.exceptions import DropItem
# from twisted.enterprise import adbapi

class JsonWriterPipeline(object):

    def open_spider(self, spider):
        self.file = open('geekbrains.json', 'w', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, spider, item):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item

# class MySQLStorePipeline(object):
#     def __init__(self):
#         self.conn = MySQLdb.connect('host', 'user', 'passwd', 'dbname', charset="utf8", use_unicode=True)
#         self.cursor = self.conn.cursor()
#
#     def process_item(self, item, spider):
#         try:
#             self.cursor.execute("""INSERT INTO example_book_store (book_name, price) VALUES (%s, %s)""",
#                                 (item['book_name'].encode('utf-8'),
#                                  item['price'].encode('utf-8')))
#             self.conn.commit()
#         except MySQLdb.Error, e:
#             print('Error {}: {}'.format(e.args[0], e.args[1]))
#             return item
#
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