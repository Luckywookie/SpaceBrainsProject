# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Join


class BrainItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    subtitle = scrapy.Field()
    description = scrapy.Field()
    logo = scrapy.Field()
    teachers = scrapy.Field()


class BrainItemLoader(ItemLoader):
    url_out = TakeFirst()
    title_out = TakeFirst()
    subtitle_out = TakeFirst()
    logo_out = TakeFirst()
    description_in = Join()
    description_out = TakeFirst()
    teachers_in = Join()
    teachers_out = TakeFirst()
    # url_out = MapCompose(lambda x: x.lower())

