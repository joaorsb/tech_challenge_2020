import scrapy
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Compose, Identity
from w3lib.html import remove_tags


class ProductItem(scrapy.Item):
    store = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    barcodes = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=Join(', ')
    )
    sku = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    brand = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    name = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    description = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    package = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    image_urls = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=Join(', ')
    )
    category = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=Join('|')
    )
    product_url = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )


class BranchProductItem(scrapy.Item):
    product_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    branch = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=TakeFirst()
    )
    stock = scrapy.Field(
        output_processor=TakeFirst()
    )
    price = scrapy.Field(
        output_processor=TakeFirst()
    )