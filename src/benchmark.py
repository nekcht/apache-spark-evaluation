# benchmark.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
This script is the main entry point of the Spark Data Structure Performance Evaluator. It handles command-line arguments,
initializes the Spark session, and executes the specified function based on the provided arguments.
"""


# standard library imports
import argparse
# pySpark library imports
from pyspark.sql import SparkSession
# local module imports
from object_factory import ObjectFactory
from schemas import get_schema
from common import get_json, update_json, hdfs_setup, save_csv, save_parquet, get_execution_times
from printer import Printer, RddPrinter, DfPrinter
from query_executor import QueryExecutor, RddQueryExecutor, DfQueryExecutor


def main():
    """
    Main function to execute the program.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--function', help='Name of the function to execute')
    parser.add_argument('-file', '--file_format', type=str, choices=['csv', 'parquet'], help='File type (csv or parquet)')
    parser.add_argument('-struct', '--data_structure', type=str, choices=['rdd', 'df'], help='Data structure (rdd or df)')
    parser.add_argument('-idx_q', '--index_query', type=int, help='Query index')
    parser.add_argument('-data', '--dataset', type=str, help='Full name of dataset in ../dataset directory (e.g mydata.tar.gz)')
    parser.add_argument('-v', '--verbose', type=int, default=0, help='Print Query results or not')
    args = parser.parse_args()

    # Start the Spark Session
    spark = SparkSession.builder.appName("Big_Data_Project").getOrCreate()
    sc = spark.sparkContext

    if args.function:
        function_name = args.function

        if function_name == 'hdfs_setup':
            dataset = args.dataset
            hdfs_setup(dataset)
        elif function_name == 'save_csv':
            dataset = args.dataset
            save_csv(dataset)
        elif function_name == 'save_parquet':
            dataset = args.dataset
            save_parquet(spark, dataset)
        elif function_name == 'get_execution_times':
            get_execution_times()
        elif function_name == 'query':
            # Retrieve arguments
            file_format = args.file_format
            data_structure = args.data_structure
            idx_query = args.index_query
            dataset = args.dataset
            verbose = args.verbose
            query(sc, spark, file_format, data_structure, idx_query, dataset, verbose)
        else:
            print('Function "{}" was not found.'.format(function_name))

    # End of Spark Session
    spark.stop()


def query(sc, spark, file_format, data_structure, idx_query, dataset, verbose):
    """
    Executes a specific query based on the provided arguments.

    Args:
        sc (pyspark.SparkContext): The Spark context.
        spark (pyspark.sql.SparkSession): The Spark session.
        file_format (str): The file format of the data (CSV or Parquet).
        data_structure (str): The data structure to use (RDD or DataFrame).
        idx_query (int): The index of the query to execute.
        dataset (str): The name of the dataset (must be present in ./datasets).
        verbose (int): Whether to print query results (1 for True, 0 for False).
    """

    # Create necessary components
    builder = ObjectFactory(sc, spark)
    data_loader = builder.dataloader_builder(dataset)
    query_exec = builder.query_executor_builder(data_structure)
    result_manager = builder.result_manager_builder()
    my_printer = builder.printer_builder(data_structure)

    # query
    # fetch data
    data = data_loader.fetch_data(idx_query, file_format, data_structure)

    # run an action to trigger lazy evaluation, in order to exclude
    # the time it takes to load the data from HDFS from the execution
    # time of the query. Remove this if you want to include the reading
    # time.
    for _, v in data.items():
        v.count()

    # execute query & measure execution time.
    result, exec_time = query_exec.transform(idx_query, data)

    # save result
    # result_manager.save_result(idx_query, data_structure, file_format, result, exec_time)

    # print result
    if verbose:
        my_printer.print_results(idx_query, result)
        print("\nExecution time: {:.2f}\n--------------------\n".format(exec_time))

if __name__ == '__main__':
    main()
