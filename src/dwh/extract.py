import csv
import os

from dotenv import load_dotenv

from src.config.logging_conf import logger
from src.config.pandas_conf import total_data_csv
from src.config.sentry_conf import SENTRY_LOGGING

load_dotenv()


def write_csv(path, data):
    with open(path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data.keys())
        writer.writerows(zip(*data.values()))
    logger.info(f"succesfully wrote csv file: {path}")


def extract_data(file_mapping, process_date):
    try:
        parent_path = os.path.join("resources", "extract")
        file_name_list = []
        total_data_list = []

        for index, base_name in file_mapping.items():
            csv_path = os.path.join(parent_path, base_name + process_date + ".csv")
            total_data = total_data_csv(csv_path)

            file_name_list.append(base_name)
            total_data_list.append(total_data)

        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)

        data = {"file_name": file_name_list, "total_record": total_data_list}
        write_csv(output_file, data=data)

    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"failed to extract data: {e}")
        raise e


if __name__ == '__main__':
    file_mapping = {'1': 'customers', '2': 'products'}

    process_date = "20241021"
    extract_data(file_mapping, process_date)
