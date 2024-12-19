import os
from datetime import datetime

from dotenv import load_dotenv

from src.config.db_conf import connect_to_db, load_queries_from_xml, execute_raw_queries
from src.config.logging_conf import logger
from src.config.pandas_conf import select_to_df, inner_join_df, df_to_sql
from src.config.sentry_conf import SENTRY_LOGGING
from src.initial_load.load import write_csv

load_dotenv()

STG_DB_URI = os.getenv("STG_DB_URI")
DWH_DB_URI = os.getenv("DWH_DB_URI")


def transform_data():
    stg_engine = connect_to_db(STG_DB_URI)
    dwh_engine = connect_to_db(DWH_DB_URI)

    logger.info("start transform resources")

    try:
        table_name_list = []
        start_date_list = []
        end_date_list = []
        total_data_list = []

        query_path = os.path.join("sql", "transform.xml")
        transform_queries = load_queries_from_xml(query_path)

        # delete resources dwh
        str_sql = transform_queries.get("delete_data_dwh")
        execute_raw_queries(str_sql, dwh_engine)

        # insert to dim_customer
        start_date = datetime.now()

        dim_customer_sql = transform_queries.get("getCustomer")
        dim_customer_df = select_to_df(dim_customer_sql, stg_engine)
        df_to_sql(dim_customer_df, "dim_customer", dwh_engine)

        end_date = datetime.now()
        total_data = len(dim_customer_df)
        table_name_list.append("dim_customer")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to dim_product
        start_date = datetime.now()

        dim_product_sql = transform_queries.get("getProduct")
        dim_product_df = select_to_df(dim_product_sql, stg_engine)
        df_to_sql(dim_product_df, "dim_product", dwh_engine)

        end_date = datetime.now()
        total_data = len(dim_product_df)
        table_name_list.append("dim_product")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to dim_seller
        start_date = datetime.now()

        dim_seller_sql = transform_queries.get("getSeller")
        dim_seller_df = select_to_df(dim_seller_sql, stg_engine)
        df_to_sql(dim_seller_df, "dim_seller", dwh_engine)

        end_date = datetime.now()
        total_data = len(dim_seller_df)
        table_name_list.append("dim_seller")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to fact_order
        start_date = datetime.now()
        order_sql = transform_queries.get("getOrderStg")
        order_df = select_to_df(order_sql, stg_engine)

        dim_customer_sql = transform_queries.get("getCustomerDwh")
        dim_customer_df = select_to_df(dim_customer_sql, dwh_engine)

        merge_order_df = inner_join_df(order_df, dim_customer_df, "customer_id")
        merge_order_df = merge_order_df.drop("customer_id", axis=1)
        df_to_sql(merge_order_df, "fact_order", dwh_engine)

        total_data = len(merge_order_df)
        end_date = datetime.now()
        table_name_list.append("fact_order")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to fact_order_item
        start_date = datetime.now()
        order_item_sql = transform_queries.get("getOrderItemStg")
        order_item_df = select_to_df(order_item_sql, stg_engine)

        fact_order_sql = transform_queries.get("getFactOrderDwh")
        fact_order_df = select_to_df(fact_order_sql, dwh_engine)

        dim_product_sql = transform_queries.get("getProductDwh")
        dim_product_df = select_to_df(dim_product_sql, dwh_engine)

        dim_seller_sql = transform_queries.get("getSellerDwh")
        dim_seller_df = select_to_df(dim_seller_sql, dwh_engine)

        merge_order_item_df = inner_join_df(order_item_df, fact_order_df, "order_id")
        merge_order_item_df = inner_join_df(merge_order_item_df, dim_product_df, "product_id")
        merge_order_item_df = inner_join_df(merge_order_item_df, dim_seller_df, "seller_id")
        merge_order_item_df = merge_order_item_df.drop(['order_id', 'product_id', 'seller_id'], axis=1)
        df_to_sql(merge_order_item_df, "fact_order_items", dwh_engine)

        total_data = len(merge_order_df)
        end_date = datetime.now()
        table_name_list.append("fact_order_items")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to fact_customer_feedback
        start_date = datetime.now()
        order_review_sql = transform_queries.get("getOrderReviewStg")
        order_review_df = select_to_df(order_review_sql, stg_engine)

        merge_order_df = inner_join_df(order_review_df, fact_order_df, "order_id")
        merge_order_df = merge_order_df.drop("order_id", axis=1)
        df_to_sql(merge_order_df, "fact_customer_feedback", dwh_engine)

        total_data = len(merge_order_df)
        end_date = datetime.now()
        table_name_list.append("fact_customer_feedback")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        # insert to fact_sales_analysis
        start_date = datetime.now()
        sales_analysis_sql = transform_queries.get("getFactSalesAnalysis")
        sales_analysis_df = select_to_df(sales_analysis_sql, stg_engine)

        merge_order_df = inner_join_df(sales_analysis_df, dim_product_df, "product_id")
        merge_order_df = merge_order_df.drop("product_id", axis=1)
        df_to_sql(merge_order_df, "fact_sales_analysis", dwh_engine)

        total_data = len(merge_order_df)
        end_date = datetime.now()
        table_name_list.append("fact_sales_analysis")
        start_date_list.append(start_date)
        end_date_list.append(end_date)
        total_data_list.append(total_data)

        extract_folder = os.path.join("resources", "transform")

        data = {
            "table_name": table_name_list,
            "start_date": start_date_list,
            "end_date": end_date_list,
            "total_data": total_data_list
        }

        csv_file_path = os.path.join(extract_folder, "status.csv")

        write_csv(csv_file_path, data=data)
    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to transform data: {e}")
        raise e


if __name__ == '__main__':
    transform_data()
