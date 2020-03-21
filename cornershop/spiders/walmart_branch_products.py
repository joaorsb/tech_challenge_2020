# -*- coding: utf-8 -*-
import json
from collections import namedtuple
import json
import scrapy
from scrapy.loader import ItemLoader
from cornershop.items import BranchProductItem
from sqlalchemy.orm import sessionmaker
from database_setup import engine
from models import Product


Branch = namedtuple('Branch', 'id latitude longitude')


class WalmartBranchProductsSpider(scrapy.Spider):
    name = 'walmart_branch_products'
    allowed_domains = ['walmart.ca']
    custom_settings = {
        'ITEM_PIPELINES': {
            'cornershop.pipelines.WalmartBranchProductPipeline': 200,
        }
    }

    def __init__(self, *args, **kwargs):
        super(WalmartBranchProductsSpider, self).__init__(*args, **kwargs)
        self.SQLSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def start_requests(self):
        """
            Hard coded coordinations for Thunder Bay(3124) and Dufferin Mall (3106)
            because they are too far away and site search didn't show them together in the Near Me results
        """
        branches = [
            Branch(3124, 48.412428, -89.240537),
            Branch(3106, 43.656186, -79.434842)
        ]
        session = self.SQLSession()
        products = session.query(Product).filter_by(store="Walmart").all()
        session.close()
        for product in products:
            product_dict = product.__dict__
            upc = product_dict['barcodes'].split(',')[0]
            for branch in branches:
                branch_url = f'https://www.walmart.ca/api/product-page/find-in-store?latitude={branch.latitude}&longitude={branch.longitude}&lang=en&upc={upc.strip()}'
                meta = {"product":  product_dict, "branch": branch}
                yield scrapy.Request(url=branch_url, callback=self.parse, meta=meta)

    def parse(self, response):
        product = response.meta["product"]
        branch = response.meta["branch"]
        try:
            branches_list = json.loads(response.text)['info']
            branch_response = [x for x in branches_list if x['id'] == branch.id][0]
            branch_loader = ItemLoader(item=BranchProductItem(), response=response)
            branch_loader.add_value('product_id', product['id'])
            branch_loader.add_value('branch', str(branch_response['id']))
            branch_loader.add_value('stock', branch_response['availableToSellQty'])
            branch_loader.add_value('price', branch_response['sellPrice'])

            yield branch_loader.load_item()
            # self.log(branch_loader.load_item())
        except KeyError:
            yield None

