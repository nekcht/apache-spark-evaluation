# Spark Data Structure Performance Evaluator

This is a simple Python-based system designed, for educational purposes, to evaluate the execution time differences between RDD (Resilient Distributed Datasets) and DataFrame data structures in Apache Spark. It also takes into account the file format being used, such as CSV or Parquet.

## Requirements

- OpenJDK 8
- Apache Hadoop 2.7.7
- Apache Spark 2.4.4
- Python 3.5.2

## Workflow
When you try to execute a query, the system typically works like this:
1. Data is fetched from the Hadoop Distributed File System (HDFS) using the data_loader.py class, considering the specified file format (CSV or Parquet).
2. The loaded data is passed to the query_executor.py class, which executes the designated query based on the user's choice of data structure (RDD or DataFrame).
3. The system measures and records the execution time for each query, providing insights into the performance differences between RDD and DataFrame data structures.

## Dataset
You can find the dataset here: https://www.dropbox.com/s/c10t67glk60wpha/datasets2023.tar.gz?dl=0.

The dataset contains the following cvs's:
1. movies.csv (id, name, description, release_year, duration, cost, revenue, popularity)
2. ratings.csv (id, movie_id, rating, timestamp)
3. movie_genres.csv (movie_id, genre)
4. employeesR.csv (employee_id, name, department_id)
5. departmentsR.csv (department_id, name)

The current implementation contains 5 pre-defined queries.

## Usage
Follow these steps:
1. Open a terminal window and navigate to './src'.
2. Download the dataset:
```bash
wget -O ../datasets/project2023.tar https://www.dropbox.com/s/c10t67glk60wpha/project2023.tar.gz?dl=0
```

3. Prepare HDFS:
```bash
spark-submit benchmark.py -f hdfs_setup -data project2023
```

4. Extract dataset and store csv's in HDFS:
```bash
spark-submit benchmark.py -f save_csv -data project2023
```

5. Convert csv's to parquet and store iN HDFS:
```bash
spark-submit benchmark.py -f save_parquet -data project2023
```

6. Run a query. The following command saves the result in '../output' dir:
```bash
spark-submit benchmark.py -f query -file csv -struct rdd -idx_q 1 -data project2023 -v 1 > ../output/result.txt
```
If you want to print the results in terminal run this command instead:
```bash
spark-submit benchmark.py -f query -file csv -struct rdd -idx_q 1 -data project2023 -v 1
```

## How to
* How to define my own queries (transformations) for a new dataset?
1. > Compress your csv's in a .tar file and store it in '/datasets' dir.
2. > Define a schema for each csv in schemas.py. Update schema_map dictionary.
3. > Define your transformations methods (e.g. query_6, query_7...etc) in query_executor.py in the parent class and it's subclasses. Update transform_map dictionary.
4. > Update query_data_map.json. The key is your query index (e.g. 6, 7..) and the value is a list with the csv's that the query requires.
5. > Define your custom printing function in printer.py in the parent class and it's subclasses. Update printer_map dictionary.
6. > You're set to go! Now follow steps 2 to 5 in "Usage", but this time instead of "project2023" use the name of your dataset (.tar) file.

