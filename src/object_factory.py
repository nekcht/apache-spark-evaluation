# object_factory.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
The ObjectFactory class provides methods to create instances of different classes, such as DataLoader, QueryExecutor,
ResultManager, and Printer. It serves as a central factory for creating these objects, taking the SparkContext and
SparkSession objects as input to initialize the classes.
"""


# local module imports
from data_loader import DataLoader
from query_executor import QueryExecutor, RddQueryExecutor, DfQueryExecutor
from result_manager import ResultManager
from printer import Printer, RddPrinter, DfPrinter


class ObjectFactory:
    """
    Creates objects of other classes.

    This class provides methods to create instances of different classes, such as DataLoader, QueryExecutor,
    ResultManager, and Printer. It takes the SparkContext and SparkSession objects as input to initialize these classes.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.

    """
    
    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark

    def dataloader_builder(self, dataset):
        """
        Creates an instance of DataLoader.

        Args:
            dataset (str): The name of the dataset.

        """
        data_loader = DataLoader(self.sc, self.spark, dataset)

        if data_loader is None:
            raise ValueError("Something went wrong. Cannot create data_loader.")

        return data_loader

    def query_executor_builder(self, data_structure):
        """
        Creates an instance of QueryExecutor based on the data structure.

        Args:
            data_structure (str): The data structure ('rdd' or 'df').

        """
        query_exec = None

        if data_structure == 'rdd':
            query_exec = RddQueryExecutor(self.sc, self.spark)
        elif data_structure == 'df':
            query_exec = DfQueryExecutor(self.sc, self.spark)

        if query_exec is None:
            raise ValueError("Something went wrong. Cannot create query_executor.")

        return query_exec

    def result_manager_builder(self):
        """
        Creates an instance of ResultManager.

        Returns:
            ResultManager: An instance of the ResultManager class.

        """
        result_manager = ResultManager(self.sc, self.spark)

        if result_manager is None:
            raise ValueError("Something went wrong. Cannot create result_manager.")

        return result_manager

    def printer_builder(self, data_structure):
        """
        Creates an instance of Printer based on the data structure.

        Args:
            data_structure (str): The data structure ('rdd' or 'df').

        """
        printer = None

        if data_structure == 'rdd':
            printer = RddPrinter(self.sc, self.spark)
        elif data_structure == 'df':
            printer = DfPrinter(self.sc, self.spark)

        if printer is None:
            raise ValueError("Something went wrong. Cannot create result_manager.")

        return printer
