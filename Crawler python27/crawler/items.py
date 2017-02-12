# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from __future__ import absolute_import
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Join


class BrainedItem(scrapy.Item):
    url = scrapy.Field()
    Rank = scrapy.Field()
    PersonID = scrapy.Field()


class BrainedItemLoader(ItemLoader):
    url_out = TakeFirst()
    Rank_in = Join(u',')
    Rank_out = TakeFirst()
    PersonID_in = Join(u',')
    PersonID_out = TakeFirst()
