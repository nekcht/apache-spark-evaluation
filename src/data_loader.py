# data_loader.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
If you want to add datasets for a new query, you have to add a new entry
in query_data_map.json and schemas.py, assuming that the required files are
already stored in HDFS:///datasets/<dataset>/csv/<csv_name>.csv for CSV files
and HDFS:///datasets/<dataset>/parquet/<csv_name> for Parquet files.
"""


# standard library imports
import os
# pySpark library imports
from pyspark.sql import SparkSession
# local module imports
from schemas import get_schema
from common import get_json


class DataLoader:
    """
    A class for loading data using Apache Spark.

    This class provides a method to fetch data from a specified data structure and file format using Apache Spark. It reads the data from HDFS and returns the loaded data as RDDs or DataFrames based on the specified data structure.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        dataset (str): The name of the dataset.

    """

    def __init__(self, sc, spark, dataset):
        """
        Initializes the DataLoader instance.

        Args:
            sc (pyspark.SparkContext): The SparkContext object.
            spark (pyspark.sql.SparkSession): The SparkSession object.
            dataset (str): The name of the dataset.

        """
        self.dataset = dataset
        self.sc = sc
        self.spark = spark

    def fetch_data(self, index_query, file_format, data_structure):
        """
        Reads data from a specified data structure and file format using Apache Spark.

        Args:
            index_query (int): The index of the query to be executed.
            file_format (str): The file format ('csv' or 'parquet') to specify how the data is stored.
            data_structure (str): The data structure ('rdd' or 'df') to specify the desired data structure.

        Returns:
            dict: A dictionary containing the loaded data as RDDs or DataFrames based on the specified data structure.

        Raises:
            ValueError: If an invalid file name is encountered.

        """

        # get configuration
        conf = get_json('paths')
        hdfs_datasets = conf['hdfs_datasets_path']
        dataset_dir = os.path.join(hdfs_datasets, self.dataset)
        dataset_csv_dir = os.path.join(dataset_dir, 'csv')
        dataset_parquet_dir = os.path.join(dataset_dir, 'parquet')

        # get all necessary file names for this query
        data_map = get_json('query_data_map')
        data_list = data_map[str(index_query)]

        output = {}
        for file_name in data_list:
            # get schema
            schema = get_schema(file_name)
            if schema is None:
                raise ValueError("Invalid file_name : {}".format(file_name))

            # fetch the data from HDFS (whether csv or parquet depends on 'data_structure')
            if file_format == 'csv':
                # read dataset
                dataset = self.spark.read \
                    .format('csv') \
                    .schema(schema) \
                    .option("mode", "DROPMALFORMED") \
                    .load(os.path.join(dataset_csv_dir, file_name + '.csv'))
            elif file_format == 'parquet':
                dataset = self.spark.read \
                    .schema(schema) \
                    .parquet(os.path.join(dataset_parquet_dir, file_name))

            if data_structure == 'rdd':
                dataset = dataset.rdd

            output[file_name] = dataset

        return output
