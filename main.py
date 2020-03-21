import csv
from collections import namedtuple, defaultdict
from typing import List, Any
from w3lib.html import remove_tags
from io import StringIO
from urllib.request import urlopen

from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker

from database_setup import engine
from models import Product, BranchProduct


ProductTuple = namedtuple('Product', 'SKU BUY_UNIT DESCRIPTION_STATUS ORGANIC_ITEM KIRLAND_ITEM FINELINE_NUMBER EAN ITEM_NAME ITEM_DESCRIPTION ITEM_IMG CATEGORY SUB_CATEGORY SUB_SUB_CATEGORY BRAND_NAME')
Price = namedtuple('Price', 'SKU BRANCH_REFERENCE PRICE_WITHOUT_IVA IVA')
Stock = namedtuple('Stock', 'SKU BRANCH_REFERENCE STOCK')
Branch = namedtuple('Branch', 'ID BRANCH_REFERENCE NAME REFERENCE')


def get_file(url: str) -> StringIO:
    data = urlopen(url).read().decode('utf-8', 'ignore')
    return StringIO(data)


def load_csv_file(csv_file: StringIO, wrapper_tuple: Any, list_type: str, branches_filter: List):
    # with open(csv_file, 'r') as origin_file:
    print(f"Downloading file")
    f_csv = csv.reader(csv_file, delimiter="|")
    _ = next(f_csv)
    for row in f_csv:
        result = wrapper_tuple(*row)
        if list_type == 'stock' and int(result.STOCK) > 0 and result.BRANCH_REFERENCE in branches_filter:
            yield result
        elif list_type == 'price' and result.BRANCH_REFERENCE in branches_filter:
            yield result
        elif list_type == 'product' or list_type == 'branch':
            yield result


def remove_items_without_price_and_stock(prices_stocks_list: List):
    for item in prices_stocks_list:
        if 'PRICE_WITHOUT_IVA' in item.keys() and 'STOCK' in item.keys():
            yield item


def prepare_categories_string(category: str, sub_category: str, sub_sub_category: str) -> str:
    return f"{category.lower()}|{sub_category.lower()}|{sub_sub_category.lower()}"


def prepare_description(description: str) -> str:
    return remove_tags(description).title()


def prepare_package_string(product_description: str) -> str:
    unit = product_description.split()[-2:]
    if unit[0][0].isdigit():
        return " ".join(unit).lstrip()
    else:
        if " " in unit[1]:
            return " ".join(unit[1]).lstrip()
        else:
            return unit[1].lstrip()


def save_product(product: Product) -> bool:
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = Session()
    product_exists = session.query(Product).filter_by(sku=product.sku).first()
    saved = True
    if product_exists is not None:
        print('Duplicate item')
    else:
        try:
            session.add(product)
            session.commit()

        except exc.SQLAlchemyError:
            session.rollback()
            saved = False
        finally:
            session.close()
    return saved


def main():
    branches_filter = ['MM', 'RHSM', 'DMS', 'HRO']
    merged = defaultdict(dict)
    merged_products_prices_stocks = defaultdict(dict)
    product_file = get_file('https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv')
    prices_file = get_file('https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/STOCK.csv')
    stocks_file = get_file('https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES.csv')

    """
        Stocks are in the wrongly named file, PRICES.csv; Prices are in the wrongly named file, STOCK.csv
    """
    products_list = load_csv_file(
        csv_file=product_file,
        wrapper_tuple=ProductTuple,
        list_type='product',
        branches_filter=branches_filter
    )
    prices_list = load_csv_file(
        csv_file=prices_file,
        wrapper_tuple=Price,
        list_type='price',
        branches_filter=branches_filter
    )
    stocks_list = load_csv_file(
        csv_file=stocks_file,
        wrapper_tuple=Stock,
        list_type='stock',
        branches_filter=branches_filter
    )

    for linked_list in (stocks_list, prices_list):
        print(f"prices and stocks merge")
        for elem in linked_list:
            merged[elem.SKU, elem.BRANCH_REFERENCE].update(elem._asdict())
            
    final_price_stock_list = remove_items_without_price_and_stock(list(merged.values()))
    final_price_stock_list = list(sorted(final_price_stock_list, key=lambda x: x['SKU']))
    products_list = list(sorted(products_list, key=lambda x: x.SKU))

    for l in (products_list, final_price_stock_list):
        for elem in l:
            if type(elem) == ProductTuple:
                merged_products_prices_stocks[elem.SKU].setdefault("product", {}).update(elem._asdict())
            else:
                merged_products_prices_stocks[elem['SKU']].setdefault("branches", []).append(elem)

    final_products_list = [
        product for product in merged_products_prices_stocks.values() if 'branches' in product.keys()
    ]

    for item in final_products_list:
        product = Product()
        product.name = item['product']['ITEM_NAME'].title()
        product.store = "Richart's"
        product.barcodes = item['product']['EAN']
        product.sku = item['product']['SKU']
        product.brand = item['product']['BRAND_NAME'].title()
        product.description = prepare_description(item['product']['ITEM_DESCRIPTION'])
        product.package = prepare_package_string(product.description)
        product.category = prepare_categories_string(
            item['product']['CATEGORY'],
            item['product']['SUB_CATEGORY'],
            item['product']['SUB_SUB_CATEGORY']
        )
        product.image_urls = item['product']['ITEM_IMG']
        product.product_url = ''
        product.branch_products = []
        for item_branch in item['branches']:
            branch_product = BranchProduct()
            branch_product.branch = item_branch['BRANCH_REFERENCE']
            branch_product.stock = item_branch['STOCK']
            branch_product.price = float(item_branch['PRICE_WITHOUT_IVA']) + (float(item_branch['PRICE_WITHOUT_IVA']) / (100 / float(item_branch['IVA'].replace("%", ''))))
            product.branch_products.append(branch_product)

        if save_product(product):
            print(f"Saved product: {item['product']['ITEM_NAME'].title()}")


if __name__ == "__main__":
    main()
