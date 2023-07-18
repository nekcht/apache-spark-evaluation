# query_processor.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
..
"""

# pySpark library imports
from pyspark.sql import SparkSession


class ResultManager:
    """
    Abstract base class for managing query results.

    The ResultManager class provides methods to save and retrieve query results along with their execution times. It serves
    as a central repository for storing the results of executed queries, facilitating easy access and retrieval.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        results (dict): A dictionary to store the query results.

    """

    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark
        self.results = {}

    def save_result(self, index_query, data_structure, file_format, result, exec_time):
        """
        Saves the result and execution time of a query.

        Args:
            index_query (int): The index of the query.
            data_structure (str): The data structure used for the result.
            file_format (str): The file format used for the result.
            result: The query result.
            exec_time (float): The execution time of the query.

        """

        self.results[index_query] = {}
        self.results[index_query][data_structure] = {}
        self.results[index_query][data_structure][file_format] = {}
        self.results[index_query][data_structure][file_format]['data'] = result
        self.results[index_query][data_structure][file_format]['exec_time'] = exec_time

        return None

    def get_result(self, index_query, data_structure, file_format):
        """
        Retrieves the result and execution time of a query.

        Args:
            index_query (int): The index of the query.
            data_structure (str): The data structure used for the result.
            file_format (str): The file format used for the result.

        Returns:
            result: The query result.
            exec_time (float): The execution time of the query.

        """
        data = self.results[index_query][data_structure][file_format]['data']
        exec_time = self.results[index_query][data_structure][file_format]['exec_time']

        return data, exec_time
