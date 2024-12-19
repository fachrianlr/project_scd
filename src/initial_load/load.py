import csv
import os
from datetime import datetime

from dotenv import load_dotenv

from src.config.db_conf import connect_to_db, execute_raw_queries, load_queries_from_xml
from src.config.logging_conf import logger
from src.config.pandas_conf import csv_to_sql, total_data_csv
from src.config.sentry_conf import SENTRY_LOGGING

load_dotenv()

DB_URI = os.getenv("STG_DB_URI")


def write_csv(path, data):
    with open(path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data.keys())
        writer.writerows(zip(*data.values()))
    logger.info(f"succesfully wrote csv file: {path}")


def load_data(file_mapping):
    # Connect to the database
    engine = connect_to_db(DB_URI)

    logger.info("start load resources")

    try:
        query_path = os.path.join("sql", "load.xml")
        load_queries = load_queries_from_xml(query_path)

        logger.info(f"delete stg resources")
        str_sql = load_queries.get("delete_stg_data")
        execute_raw_queries(str_sql, engine)

        extract_folder = os.path.join("resources", "extract")
        load_folder = os.path.join("resources", "load")

        csv_name_list = []
        start_date_list = []
        end_date_list = []
        total_data_list = []
        for csv_name, table_name in file_mapping.items():
            start_date = datetime.now()

            csv_path = os.path.join(extract_folder, csv_name)
            csv_to_sql(csv_path, table_name, engine)

            end_date = datetime.now()

            csv_name_list.append(csv_name)
            start_date_list.append(start_date)
            end_date_list.append(end_date)
            total_data_list.append(total_data_csv(csv_path))

        data = {"csv_name": csv_name_list, "start_date": start_date_list, "end_date": end_date_list,
                "total_data": total_data_list}

        csv_file_path = os.path.join(load_folder, "status.csv")

        write_csv(csv_file_path, data=data)

    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to load data: {e}")
        raise e


if __name__ == '__main__':
    file_mapping = {
        'customers.csv': 'customers',
        'products.csv': 'products',
        'sellers.csv': 'sellers',
        'geolocation.csv': 'geolocation',
        'orders.csv': 'orders',
        'order_items.csv': 'order_items',
        'order_payments.csv': 'order_payments',
        'order_reviews.csv': 'order_reviews',
        'product_category_name_translation.csv': 'product_category_name_translation'
    }

    load_data(file_mapping)
