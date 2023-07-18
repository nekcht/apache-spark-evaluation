# printer.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
If you need to add a new printing function for your query, make sure to define it for both RDD and DataFrame,
and then update the printer_map dictionary accordingly. This approach ensures consistency and allows for easy
extension of printing functionality for new queries.
"""


# standard library imports
from abc import ABC, abstractmethod
# pySpark library imports
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Printer(ABC):
    """
    An abstract base class for printing query results.

    This abstract class defines the common interface for printing query results. Subclasses must implement the abstract
    methods for each specific query result printing.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        printer_map (dict): A dictionary mapping query indices to their respective printing methods.

    """
    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark
        self.printer_map = {
            1: self.print_query_1,
            2: self.print_query_2,
            3: self.print_query_3,
            4: self.print_query_4,
            5: self.print_query_5
        }

    @abstractmethod
    def print_query_1(self, result):
        pass

    @abstractmethod
    def print_query_2(self, result):
        pass

    @abstractmethod
    def print_query_3(self, result):
        pass

    @abstractmethod
    def print_query_4(self, result):
        pass

    @abstractmethod
    def print_query_5(self, result):
        pass

    def print_results(self, idx_query, data):
        """
        Prints the results of a specified query.

        Args:
            idx_query (int): The index of the query.
            data: The data to be printed.

        """

        printer = self.printer_map[idx_query]
        printer(data)

        return None


class RddPrinter(Printer):
    """
    A class for printing query results based on RDD data structure.

    This class extends the abstract Printer class and implements the printing methods for each query result based on RDDs.

    """
    def __init__(self, sc, spark):
        super().__init__(sc, spark)

    def print_query_1(self, result):
        for item in result:
            key = item[0]
            values = item[1]
            print("Year: {}".format(key))
            print("  Movie - Profit")
            for value in values:
                movie = value[0]
                profit = value[1]
                print("  " + movie + " - " + str(profit))
            print()  # Add an empty line between groups

        return None

    def print_query_2(self, result):
        movie_id = result[0]
        num_users_rated = result[1]
        avg_rating = result[2]

        print("Movie ID: {}".format(movie_id))
        print("Number of users rated: {}".format(num_users_rated))
        print("Average rating: {}".format(avg_rating))

        return None

    def print_query_3(self, result):
        print("Movie Name:", result[0])
        print("Movie Revenue:", result[1])

        return None

    def print_query_4(self, result):
        for item in result:
            release_year = item[0]
            movie = item[1][0]
            popularity = item[1][1]
            print("{}: {}, {:.2f}".format(release_year, movie, popularity))

        return None

    def print_query_5(self, result):
        for item in result:
            release_year = item[0]
            average_revenue = item[1]
            print("{}: {:.2f}".format(release_year, average_revenue))

        return None


class DfPrinter(Printer):
    """
    A class for printing query results based on DataFrame data structure.

    This class extends the abstract Printer class and implements the printing methods for each query result based on DataFrames.

    """
    def __init__(self, sc, spark):
        super().__init__(sc, spark)

    def print_query_1(self, result):
        for row in result:
            release_year = row.release_year
            movie_profits = row.MovieProfit
            print(release_year)
            for movie_profit in movie_profits:
                movie = movie_profit.name
                profit = movie_profit.col2
                print("  " + movie + " - " + str(profit))
            print()  # Add an empty line between groups

        return None

    def print_query_2(self, result):
        movie_id = result[0][0]
        num_users_rated = result[0][1]
        avg_rating = result[0][2]

        print("Movie ID: {}".format(movie_id))
        print("Number of users rated: {}".format(num_users_rated))
        print("Average rating: {}".format(avg_rating))

        return None

    def print_query_3(self, result):
        print("Movie Name:", result[0][0])
        print("Movie Revenue:", result[0][1])

        return None

    def print_query_4(self, result):
        for row in result:
            release_year = row.release_year
            movie = row.name
            popularity = row.popularity
            print("{}: {}, {:.2f}".format(release_year, movie, popularity))

        return None

    def print_query_5(self, result):
        for row in result:
            release_year = row.release_year
            avg_revenue = row.avg_revenue
            print("{}: {:.2f}".format(release_year, avg_revenue))

        return None
