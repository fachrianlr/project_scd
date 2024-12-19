import csv
import os

from dotenv import load_dotenv

from src.config.db_conf import connect_to_db, load_queries_from_xml, execute_raw_queries, select_raw_query
from src.config.logging_conf import logger
from src.config.pandas_conf import csv_to_df
from src.config.sentry_conf import SENTRY_LOGGING

load_dotenv()

DB_URI = os.getenv("STG_DB_URI")


def write_csv(path, data):
    with open(path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data.keys())
        writer.writerows(zip(*data.values()))
    logger.info(f"succesfully wrote csv file: {path}")


def insertorupdate_customer(load_queries, process_date, engine):
    extract_folder = os.path.join("resources", "extract")
    customer_name = "customers" + process_date + ".csv"
    customer_file = os.path.join(extract_folder, customer_name)
    try:
        if os.path.exists(customer_file):
            customer_data = csv_to_df(customer_file)
            for _, row in customer_data.iterrows():
                logger.info(f"data : {row}")
                str_sql = load_queries.get("getCustomerById")
                params_id = {"customer_id": row['customer_id']}
                fetch_data = select_raw_query(str_sql, engine, params=params_id)

                params_data = {'customer_id': row['customer_id'], 'customer_unique_id': row['customer_unique_id'],
                               'customer_zip_code_prefix': row['customer_zip_code_prefix'],
                               'customer_city': row['customer_city'], 'customer_state': row['customer_state']}
                if fetch_data:
                    logger.info("exist")
                    str_sql = load_queries.get("updateCustomerById")
                    execute_raw_queries(str_sql, engine, params=params_data)
                else:
                    logger.info("not exist")
                    # Insert query for new customer
                    str_sql = load_queries.get("insertCustomer")  # Make sure you have this query defined
                    execute_raw_queries(str_sql, engine, params=params_data)
        else:
            logger.info("nothing to be updated")
    except Exception as e:
        logger.error(f"failed to load data: {e}")
        raise e


def insertorupdate_product(load_queries, process_date, engine):
    extract_folder = os.path.join("resources", "extract")
    product_name = "products" + process_date + ".csv"
    product_file = os.path.join(extract_folder, product_name)
    try:
        if os.path.exists(product_file):
            product_data = csv_to_df(product_file)
            for _, row in product_data.iterrows():
                logger.info(f"data : {row}")
                str_sql = load_queries.get("getProductById")
                params_id = {"product_id": row['product_id']}
                fetch_data = select_raw_query(str_sql, engine, params=params_id)

                params_data = {
                    'product_id': row['product_id'],
                    'product_category_name': row['product_category_name'],
                    'product_name_lenght': row['product_name_lenght'],
                    'product_description_lenght': row['product_description_lenght'],
                    'product_photos_qty': row['product_photos_qty'],
                    'product_weight_g': row['product_weight_g'],
                    'product_length_cm': row['product_length_cm'],
                    'product_height_cm': row['product_height_cm'],
                    'product_width_cm': row['product_width_cm']
                }

                if fetch_data:
                    logger.info("exist")
                    str_sql = load_queries.get("updateProductById")
                    execute_raw_queries(str_sql, engine, params=params_data)
                else:
                    logger.info("not exist")
                    # Insert query for new customer
                    str_sql = load_queries.get("insertProduct")
                    execute_raw_queries(str_sql, engine, params=params_data)
        else:
            logger.info("nothing to be updated")
    except Exception as e:
        logger.error(f"failed to load data: {e}")
        raise e


def load_data(process_date):
    # Connect to the database
    engine = connect_to_db(DB_URI)

    logger.info("start load resources")

    query_path = os.path.join("sql", "load.xml")
    load_queries = load_queries_from_xml(query_path)
    parent_path = os.path.join("resources", "load")

    try:
        insertorupdate_customer(load_queries, process_date, engine)
        insertorupdate_product(load_queries, process_date, engine)

        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)
        with open(output_file, "w") as f:
            f.write("success")
    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to load data: {e}")
        raise e


if __name__ == '__main__':
    process_date = "20241021"
    load_data(process_date)
