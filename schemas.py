# schemas.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
Here you can add new schemas for the datasets of your query.
First define the schema, then add a new entry in schema_map.
Key should be the name of the dataset and value the schema.
"""

from pyspark.sql.types import *

# schema for the movie dataset
moviesSchema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("description", StringType(), nullable=False),
    StructField("release_year", IntegerType(), nullable=False),
    StructField("duration", IntegerType(), nullable=False),
    StructField("cost", LongType(), nullable=False),
    StructField("revenue", LongType(), nullable=False),
    StructField("popularity", FloatType(), nullable=False)
])

# schema for the ratings dataset
ratingsSchema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("movie_id", IntegerType(), nullable=False),
    StructField("rating", FloatType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
])

# schema for the movie_genres dataset
genresSchema = StructType([
    StructField("movie_id", IntegerType(), nullable=False),
    StructField("genre", StringType(), nullable=False),
])

# schema for the movie_genres dataset
employeesSchema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False)
])

# schema for the movie_genres dataset
departmentSchema = StructType([
    StructField("department_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
])

"""
 A mapping of datasets to their corresponding schema.
"""
schema_map = {
    'movies': moviesSchema,
    'ratings': ratingsSchema,
    'movie_genres': genresSchema,
    'employeesR': employeesSchema,
    'departmentsR': departmentSchema
}


def get_schema(dataset):
    """
    Retrieves the schema for a given dataset.

    Args:
        dataset (str): The name of the dataset.

    Returns:
        schema (pyspark.sql.types.StructType): The schema of the dataset.

    """

    schema = schema_map[dataset]

    return schema
