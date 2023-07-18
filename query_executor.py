# query_executor.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
All transformations for all available queries are defined here. For each
query, there's an implementation in RddQueryExecutor and in DfQueryExecutor.
If you wish to add your own methods for a query, make sure you update the transform_map dict.
"""

# standard library imports
import time
import builtins
from abc import ABC, abstractmethod
# pySpark library imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
# local module imports


class QueryExecutor(ABC):
    """
    A class for processing queries using transformations as functions.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        data_loader (DataLoader): A DataLoader object that contains the data.

    """

    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark
        self.transform_map = {
            1: self.query_1,
            2: self.query_2,
            3: self.query_3,
            4: self.query_4,
            5: self.query_5
        }

    @abstractmethod
    def query_1(self, data):
        """
        For every year after 1995 print the difference between the money spent
        to create the movie and the revenue of the movie (revenue – production cost).
        """
        pass

    @abstractmethod
    def query_2(self, data):
        """
        For the movie “Cesare deve morire” find and print the movies id and then search
        how many users rated the movie and what the average rating was?
        """
        pass

    @abstractmethod
    def query_3(self, data):
        """
        What was the best in terms of revenue Animation movie of 1995?
        """
        pass

    @abstractmethod
    def query_4(self, data):
        """
        Find and print the most popular Comedy movies for every year after 1995.
        """
        pass

    @abstractmethod
    def query_5(self, data):
        """
        For every year print the average movie revenue.
        """
        pass

    def transform(self, idx_query, data):
        """
        Process a query using the provided transformation function from self.transform_func
        and data from data_loader.data.

        Args:
            idx_query (int): The index of the query.
            data: The input data for the query.

        Returns:
            result, exec_time: The result of applying the transformations to the data and the
                               execution time.

        """

        f = self.transform_map[idx_query]

        start_time = time.time()

        result = f(data).collect()

        end_time = time.time()
        execution_time = end_time - start_time

        if result is None:
            raise ValueError("Transformations error")

        return result, execution_time


class RddQueryExecutor(QueryExecutor):
    """
    A class for processing queries using transformations as functions on RDD data.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        data_loader (DataLoader): A DataLoader object that contains the data.

    """

    def __init__(self, sc, spark):
        super().__init__(sc, spark)

    def query_1(self, data):
        # Unpack the datasets into variables
        movies = data['movies']

        # Transformations
        result = movies.filter(lambda row: (row['revenue'] > 0) and (row['cost'] > 0))\
            .filter(lambda row: row['release_year'] > 1995) \
            .map(lambda row: (row['release_year'], (row['name'], row['revenue'] - row['cost']))) \
            .groupByKey() \
            .sortByKey()

        return result

    def query_2(self, data):
        # Unpack the datasets into variables

        movies = data['movies']
        ratings = data['ratings']

        # Transformations
        movie_id = movies.filter(
            lambda row: row['name'] == 'Cesare deve morire'
        ).map(lambda row: row[0]).first()
        ratings_filtered = ratings.filter(lambda row: row['movie_id'] == movie_id)
        num_users_rated = ratings_filtered.count()
        total_rating = ratings_filtered.map(lambda row: row['rating']).sum()
        average_rating = builtins.round(total_rating / num_users_rated, 2)

        temp_result = [movie_id, num_users_rated, average_rating]
        result = self.sc.parallelize(temp_result)

        return result

    def query_3(self, data):
        # Unpack the datasets into variables
        movies = data['movies']
        genres = data['movie_genres']

        # Transformations
        movies_1995 = movies.filter(lambda row: (row['revenue'] > 0) and (row['cost'] > 0))\
            .filter(lambda row: row['release_year'] == 1995)
        animation_movies_ids = genres.filter(lambda row: row['genre'] == 'Animation') \
            .map(lambda row: int(row[0])).collect()
        temp_result = movies_1995.filter(lambda row: row['id'] in animation_movies_ids)\
            .map(lambda row: (row['name'], row['revenue'])) \
            .sortBy(lambda x: x[1], ascending=False).first()

        result = self.sc.parallelize(temp_result)

        return result

    def query_4(self, data):
        # Unpack the datasets into variables
        movies = data['movies']
        genres = data['movie_genres']

        # Transformations
        movies_after_1995 = movies.filter(lambda row: (row['revenue'] > 0) and (row['cost'] > 0))\
            .filter(lambda row: row['release_year'] > 1995)
        comedy_movies_ids = genres.filter(lambda row: row['genre'] == 'Comedy') \
            .map(lambda row: int(row[0])).collect()
        result = movies_after_1995.filter(lambda row: row['id'] in comedy_movies_ids)\
            .map(lambda row: (row['release_year'], (row['popularity'], row))) \
            .reduceByKey(lambda x, y: x if x[0] >= y[0] else y) \
            .map(lambda x: x[1][1]) \
            .map(lambda row: (row['release_year'], (row['name'], row['popularity']))) \
            .sortByKey()

        return result

    def query_5(self, data):
        # Unpack the datasets into variables
        movies = data['movies']

        # Transformations
        result = movies.filter(lambda row: (row['revenue'] > 0) and (row['release_year'] > 0))\
            .map(lambda row: (row['release_year'], (row['revenue'], 1)))\
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .mapValues(lambda x: x[0] / x[1]).sortByKey()

        return result


class DfQueryExecutor(QueryExecutor):
    """
    A class for processing queries using transformations as functions on DataFrame data.

    Attributes:
        sc (pyspark.SparkContext): The SparkContext object.
        spark (pyspark.sql.SparkSession): The SparkSession object.
        data_loader (DataLoader): A DataLoader object that contains the data.

    """

    def __init__(self, sc, spark):
        super().__init__(sc, spark)

    def query_1(self, data):
        # Unpack the datasets into variables
        movies = data['movies']

        # Transformations
        movies.createOrReplaceTempView("movies")
        query = """
        SELECT
            release_year,
            COLLECT_LIST(STRUCT(name, revenue - cost)) AS MovieProfit
        FROM
            movies
        WHERE
            cost > 0 AND revenue > 0 AND release_year > 1995
        GROUP BY
            release_year
        ORDER BY
            release_year
        """
        result = self.spark.sql(query)

        return result

    def query_2(self, data):
        # Unpack the datasets into variables
        movies = data['movies']
        ratings = data['ratings']

        # Transformations
        movies.createOrReplaceTempView("movies")
        ratings.createOrReplaceTempView("ratings")
        query = """
        SELECT
            movie_id,
            COUNT(*) AS num_users_rated,
            AVG(rating) AS average_rating
        FROM
            movies
        JOIN
            ratings ON movies.id = ratings.movie_id
        WHERE
            movies.name = "Cesare deve morire"
        GROUP BY
            movie_id
        """

        result = self.spark.sql(query)

        return result

    def query_3(self, data):
        # Unpack the datasets into variables
        movies = data['movies']
        genres = data['movie_genres']

        # Transformations
        genres.createOrReplaceTempView("genres")
        movies.createOrReplaceTempView("movies")
        query = """
            SELECT movies.name AS best_movie_title, movies.revenue AS best_movie_revenue
            FROM movies
            JOIN genres ON movies.id = genres.movie_id
            WHERE movies.release_year = 1995 AND genres.genre = 'Animation'
            ORDER BY movies.revenue DESC
            LIMIT 1
            """
        result = self.spark.sql(query)

        return result

    def query_4(self, data):
        # Unpack the datasets into variables
        movies = data['movies']
        genres = data['movie_genres']

        # Transformations
        movies_filtered = movies.filter((movies.revenue > 0) & (movies.cost > 0))
        movies_filtered.createOrReplaceTempView("movies")
        genres.createOrReplaceTempView("genres")
        query = """
            SELECT m.release_year, m.name, m.popularity
            FROM movies AS m
            JOIN genres AS g ON m.id = g.movie_id
            WHERE m.release_year > 1995 AND g.genre = 'Comedy'
            AND m.popularity IN (
                SELECT MAX(m2.popularity)
                FROM movies AS m2
                JOIN genres AS g2 ON m2.id = g2.movie_id
                WHERE m2.release_year > 1995 AND g2.genre = 'Comedy'
                GROUP BY m2.release_year
            )
            ORDER BY release_year ASC
        """
        result = self.spark.sql(query)

        return result

    def query_5(self, data):
        # Unpack the datasets into variables
        movies = data['movies']

        # Transformations
        movies.createOrReplaceTempView("movies")
        query = """
            SELECT release_year, AVG(revenue) AS avg_revenue
            FROM movies
            WHERE release_year > 0 AND revenue > 0
            GROUP BY release_year
            ORDER BY release_year ASC
        """
        result = self.spark.sql(query)

        return result