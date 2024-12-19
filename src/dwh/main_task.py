import json
import os

import luigi
from luigi import build

from src.dwh.extract import extract_data
from src.dwh.load import load_data
from src.dwh.transform import transform_data


class ExtractTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    process_date = luigi.Parameter()

    def output(self):
        parent_path = os.path.join("resources", "extract")
        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)
        return luigi.LocalTarget(output_file)

    def run(self):
        file_mapping_dict = json.loads(extract_file_mapping)
        extract_data(file_mapping_dict, self.process_date)


class LoadTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    process_date = luigi.Parameter()

    def requires(self):
        return ExtractTask(extract_file_mapping=self.extract_file_mapping, process_date=self.process_date)

    def output(self):
        parent_path = os.path.join("resources", "load")
        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)
        return luigi.LocalTarget(output_file)

    def run(self):
        load_data(self.process_date)


class TransformTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    process_date = luigi.Parameter()

    def requires(self):
        return LoadTask(extract_file_mapping=self.extract_file_mapping, process_date=self.process_date)

    def output(self):
        parent_path = os.path.join("resources", "transform")
        output_status = "status" + process_date + ".csv"
        output_file = os.path.join(parent_path, output_status)
        return luigi.LocalTarget(output_file)

    def run(self):
        transform_data(self.process_date)


if __name__ == '__main__':
    extract_file_mapping = json.dumps({
        '1': 'customers',
        '2': 'products'
    })

    process_date = "20241019"
    build(
        [TransformTask(extract_file_mapping=extract_file_mapping, process_date=process_date)], local_scheduler=True)
