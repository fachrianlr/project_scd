import os
from src.config.sentry_conf import SENTRY_LOGGING
from dotenv import load_dotenv

from src.config.db_conf import connect_to_db, load_queries_from_xml
from src.config.logging_conf import logger
from src.config.pandas_conf import select_to_df, save_to_csv

load_dotenv()

DB_URI = os.getenv("SRC_DB_URI")


def extract_data(file_mapping):
    try:
        engine = connect_to_db(DB_URI)
        query_path = os.path.join("sql", "extract.xml")
        extract_queries = load_queries_from_xml(query_path)

        logger.info("start extract resources")

        for query_id, csv_name in file_mapping.items():
            str_sql = extract_queries.get(query_id)
            logger.info(f"query : {str_sql}")
            df = select_to_df(str_sql, engine)
            csv_path = os.path.join("resources", "extract", csv_name)
            save_to_csv(df, csv_path)

    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to extract data: {e}")
        raise e


if __name__ == '__main__':
    file_mapping = {
        'get_customers': 'customers.csv',
        'get_products': 'products.csv',
        'get_sellers': 'sellers.csv',
        'get_geolocation': 'geolocation.csv',
        'get_order_items': 'order_items.csv',
        'get_order_payments': 'order_payments.csv',
        'get_order_reviews': 'order_reviews.csv',
        'get_orders': 'orders.csv',
        'get_product_category_name_translation': 'product_category_name_translation.csv'
    }
    extract_data(file_mapping)
