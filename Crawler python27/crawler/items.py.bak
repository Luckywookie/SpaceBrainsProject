# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Join


class BrainedItem(scrapy.Item):
    url = scrapy.Field()
    Rank = scrapy.Field()
    PersonID = scrapy.Field()


class BrainedItemLoader(ItemLoader):
    url_out = TakeFirst()
    Rank_in = Join(',')
    Rank_out = TakeFirst()
    PersonID_in = Join(',')
    PersonID_out = TakeFirst()
