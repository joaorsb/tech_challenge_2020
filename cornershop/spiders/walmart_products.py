# -*- coding: utf-8 -*-
import json
import scrapy
from scrapy.exceptions import CloseSpider
from scrapy_selenium import SeleniumRequest
from scrapy.loader import ItemLoader
from cornershop.items import ProductItem


class WalmartProductsSpider(scrapy.Spider):
    name = 'walmart_products'
    allowed_domains = ['walmart.ca']
    custom_settings = {
        'ITEM_PIPELINES': {
            'cornershop.pipelines.WalmartProductDupeFilterPipeline': 100,
            'cornershop.pipelines.WalmartProductPipeline': 200,
        }
    }

    def start_requests(self):
        urls = ['https://www.walmart.ca/en/grocery/N-117']

        for url in urls:
            yield SeleniumRequest(url=url, callback=self.parse_categories, wait_time=1)

    def parse_categories(self, response):
        categories_links = response.selector.xpath('//div[contains(@class, "tileGenV2_wrapper  tenCol ")]/div')
        for category_link in categories_links:
            link = category_link.xpath('./a/@href').get()
            current_category = category_link.xpath('./a/article/@title').get()
            meta = {'categories': ['Grocery', current_category]}
            yield SeleniumRequest(
                url=f'https://walmart.ca{link}',
                callback=self.parse_sub_categories,
                meta=meta,
                wait_time=1
                )

    def parse_sub_categories(self, response):
        categories = response.meta['categories']
        sub_categories_links = response.selector.xpath('//div[contains(@class, "tileGenV2_wrapper  eightCol")]/div')

        for sub_link in sub_categories_links:
            link = sub_link.xpath('./a/@href').get()
            sub_category = sub_link.xpath('./a/article/@title').get()
            head, tail = categories
            meta = {'categories': [head, tail, sub_category]}
            yield SeleniumRequest(
                url=f'https://walmart.ca{link}',
                callback=self.parse_products,
                meta=meta
            )

    def parse_products(self, response):
        categories = response.meta['categories']
        products = response.selector.xpath('//a[contains(@class, "product-link")]')

        for product in products:
            link = product.xpath('./@href').get()
            yield scrapy.Request(
                url=f'https://walmart.ca{link}',
                callback=self.parse_product,
                meta={"categories": categories}
            )

    def parse_product(self, response):
        categories = response.meta['categories']
        product_response_script = response.selector.xpath(
            '//script[contains(., "Product")]/text()'
        ).get()

        aditional_product_script = response.selector.xpath(
            '//script[contains(., "activeSkuId")]/text()'
        ).get().replace('window.__PRELOADED_STATE__=', '').replace(';', '')

        product_data = json.loads(product_response_script)
        aditional_product_data = json.loads(aditional_product_script)
        sku = product_data['sku']
        upcs = aditional_product_data['entities']['skus'][sku]['upc']

        product_loader = ItemLoader(item=ProductItem(), response=response)
        product_loader.add_value('store', 'Walmart')
        product_loader.add_value('name', product_data['name'])
        product_loader.add_value('sku', sku)
        product_loader.add_value('package', aditional_product_data['product']['item']['description'])
        product_loader.add_value('brand', product_data['brand']['name'])
        product_loader.add_value('description', product_data['description'])
        product_loader.add_value('image_urls', product_data['image'])
        product_loader.add_value('product_url', response.request.url)
        product_loader.add_value('barcodes', upcs)
        product_loader.add_value('category', categories)
        # product = product_loader.load_item()
        yield product_loader.load_item()
        # self.log(product)
