import json
import os

import luigi
from luigi import build

from src.initial_load.extract import extract_data
from src.initial_load.load import load_data
from src.initial_load.transform import transform_data


class ExtractTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()

    def output(self):
        file_mapping_dict = json.loads(extract_file_mapping)
        return {csv_name: luigi.LocalTarget(os.path.join("resources", "extract", csv_name)) for
                _, csv_name in file_mapping_dict.items()}

    def run(self):
        file_mapping_dict = json.loads(extract_file_mapping)
        extract_data(file_mapping_dict)


class LoadTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    load_file_mapping = luigi.Parameter()

    def requires(self):
        return ExtractTask(extract_file_mapping=self.extract_file_mapping)

    def output(self):
        return luigi.LocalTarget(os.path.join("resources", "load", "status.csv"))

    def run(self):
        file_mapping_dict = json.loads(load_file_mapping)
        load_data(file_mapping_dict)


class TransformTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    load_file_mapping = luigi.Parameter()

    def requires(self):
        return LoadTask(extract_file_mapping=self.extract_file_mapping, load_file_mapping=self.load_file_mapping)

    def output(self):
        return luigi.LocalTarget(os.path.join("resources", "transform", "status.csv"))

    def run(self):
        transform_data()


if __name__ == '__main__':
    extract_file_mapping = json.dumps({
        'get_customers': 'customers.csv',
        'get_products': 'products.csv',
        'get_sellers': 'sellers.csv',
        'get_geolocation': 'geolocation.csv',
        'get_order_items': 'order_items.csv',
        'get_order_payments': 'order_payments.csv',
        'get_order_reviews': 'order_reviews.csv',
        'get_orders': 'orders.csv',
        'get_product_category_name_translation': 'product_category_name_translation.csv'
    })

    load_file_mapping = json.dumps({
        'customers.csv': 'customers',
        'products.csv': 'products',
        'sellers.csv': 'sellers',
        'geolocation.csv': 'geolocation',
        'orders.csv': 'orders',
        'order_items.csv': 'order_items',
        'order_payments.csv': 'order_payments',
        'order_reviews.csv': 'order_reviews',
        'product_category_name_translation.csv': 'product_category_name_translation'
    })

    build(
        [TransformTask(extract_file_mapping=extract_file_mapping, load_file_mapping=load_file_mapping)],
        local_scheduler=True)
