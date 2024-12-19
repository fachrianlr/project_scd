import os

from dotenv import load_dotenv

from src.config.db_conf import connect_to_db, select_raw_query, load_queries_from_xml, execute_raw_queries
from src.config.logging_conf import logger
from src.config.sentry_conf import SENTRY_LOGGING

load_dotenv()

STG_DB_URI = os.getenv("STG_DB_URI")
DWH_DB_URI = os.getenv("DWH_DB_URI")


def insertorupdate_customer(load_queries, process_date, stg_engine, dwh_engine):
    logger.info("start insertorupdate_customer")
    try:
        str_sql = load_queries.get("getCustomerByWritedDateStg")
        params_id = {"process_date": process_date}
        fetch_data = select_raw_query(str_sql, stg_engine, params=params_id)
        for data in fetch_data:
            logger.info(f"Data {data}")
            str_sql = load_queries.get("getCustomerByIdDwh")
            params_id = {"customer_id": data.get("customer_id")}
            customer_data = select_raw_query(str_sql, dwh_engine, params=params_id)

            params = {
                'customer_id': data.get("customer_id"),
                'customer_unique_id': data.get("customer_unique_id"),
                'customer_zip_code_prefix': data.get("customer_zip_code_prefix"),
                'customer_city': data.get('customer_city'),
                'customer_state': data.get('customer_state'),
                'effective_end_date': None,
                'is_active': True
            }

            if customer_data:
                logger.info(f"Customer data exist")
                logger.info("Set non active for current customer and insert the new one")

                str_sql = load_queries.get("nonActiveCustomerDwh")
                params_id = {"customer_id": data.get("customer_id")}
                execute_raw_queries(str_sql, dwh_engine, params_id)

                logger.info("Inserting data customer")
                str_sql = load_queries.get("insertCustomerDwh")
                execute_raw_queries(str_sql, dwh_engine, params)
            else:
                logger.info(f"Customer data does not exist")
                logger.info("Inserting data customer")
                str_sql = load_queries.get("insertCustomerDwh")
                execute_raw_queries(str_sql, dwh_engine, params)

    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to load data: {e}")
        raise e


def insertorupdate_product(load_queries, process_date, stg_engine, dwh_engine):
    logger.info("start insertorupdate_product")
    try:
        str_sql = load_queries.get("getProductByWritedDateStg")
        params_id = {"process_date": process_date}
        fetch_data = select_raw_query(str_sql, stg_engine, params=params_id)
        for data in fetch_data:
            logger.info(f"Updated data {data}")
            str_sql = load_queries.get("getProductByIdDwh")
            params_id = {"product_id": data.get("product_id")}
            product_data = select_raw_query(str_sql, dwh_engine, params=params_id)
            logger.info(f"get product data by id : {product_data}")

            params = {
                'product_id': data.get("product_id"),
                'product_category_name': data.get("product_category_name"),
                'product_name_length': data.get("product_name_lenght"),
                'product_description_length': data.get('product_description_lenght'),
                'product_photos_qty': data.get('product_photos_qty'),
                'product_weight_g': data.get('product_weight_g'),
                'product_length_cm': data.get('product_weight_g'),
                'product_height_cm': data.get('product_height_cm'),
                'product_width_cm': data.get('product_width_cm')
            }
            #
            if product_data:
                logger.info(f"Product data exist")

                product_data = product_data[0]
                if (data['product_category_name'] != product_data['product_category_name'] or
                        data['product_weight_g'] != product_data['product_weight_g'] or
                        data['product_length_cm'] != product_data['product_length_cm'] or
                        data['product_height_cm'] != product_data['product_height_cm'] or
                        data['product_width_cm'] != product_data['product_width_cm']):

                    logger.info("update data product scd 2")
                    str_sql = load_queries.get("nonActiveProductDwh")
                    params_id = {"product_id": data.get("product_id")}
                    execute_raw_queries(str_sql, dwh_engine, params_id)

                    logger.info("Inserting data product")
                    str_sql = load_queries.get("insertProductDwh")
                    execute_raw_queries(str_sql, dwh_engine, params)
                elif (data['product_name_lenght'] != product_data['product_name_length'] or
                      data['product_description_lenght'] != product_data['product_description_length'] or
                      data['product_photos_qty'] != product_data['product_photos_qty']):
                    logger.info("update data product scd 1")
                    str_sql = load_queries.get("updateProductDwh")
                    execute_raw_queries(str_sql, dwh_engine, params)
                else:
                    logger.info(f"Nothing to be updated")
            else:
                logger.info(f"Product data does not exist")
                logger.info("Inserting data product")
                str_sql = load_queries.get("insertProductDwh")
                execute_raw_queries(str_sql, dwh_engine, params)

    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to load data: {e}")
        raise e


def transform_data(process_date):
    stg_engine = connect_to_db(STG_DB_URI)
    dwh_engine = connect_to_db(DWH_DB_URI)

    logger.info("start transform resources")
    query_path = os.path.join("sql", "transform.xml")
    load_queries = load_queries_from_xml(query_path)

    parent_path = os.path.join("resources", "transform")
    try:
        insertorupdate_customer(load_queries, process_date, stg_engine, dwh_engine)
        insertorupdate_product(load_queries, process_date, stg_engine, dwh_engine)
        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)
        with open(output_file, "w") as f:
            f.write("success")
    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to transform data: {e}")
        raise e


if __name__ == '__main__':
    process_date = "20241021"
    transform_data(process_date)
