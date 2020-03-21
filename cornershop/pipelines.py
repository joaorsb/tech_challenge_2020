from database_setup import engine
from models import Product, BranchProduct
from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
import logging


class WalmartProductDupeFilterPipeline(object):
    def __init__(self):
        self.Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def process_item(self, item, spider):
        session = self.Session()
        product_exists = session.query(Product).filter_by(sku=item['sku']).first()

        if product_exists is not None:
            raise DropItem('Duplicate item')

        session.close()


class WalmartProductPipeline(object):
    def __init__(self):
        self.Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def process_item(self, item, spider):
        session = self.Session()
        product = Product()
        product.store = item['store']
        product.barcodes = item['barcodes']
        product.sku = item['sku']
        product.brand = item['brand']
        product.name = item['name']
        product.description = item['description']
        product.package = item['package']
        product.category = item['category']
        product.image_urls = item['image_urls']
        product.product_url = item['product_url']

        try:
            session.add(product)
            session.commit()
        except exc.SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()

        return item


class WalmartBranchProductPipeline(object):
    def __init__(self):
        self.Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def process_item(self, item, spider):
        session = self.Session()
        # value_exists = session.query(
        #     BranchProduct
        # ).filter_by(
        #     product_id=item['product_id'], branch=item["branc"]
        # ).first()

        branch_product = BranchProduct()
        branch_product.product_id = item['product_id']
        branch_product.branch = item['branch']
        branch_product.stock = item['stock']
        branch_product.price = item['price']

        try:
            session.add(branch_product)
            session.commit()
        except exc.SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()

        return item
