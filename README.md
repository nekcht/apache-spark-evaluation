# Spark Data Structure Performance Evaluator

The Spark Data Structure Performance Evaluator is a Python-based system designed to evaluate the execution time differences between RDD (Resilient Distributed Datasets) and DataFrame data structures in Apache Spark. It also takes into account the file format being used, such as CSV or Parquet.

## Requirements

- openjdk-8-jdk
- hadoop-2.7.7
- spark-2.4.4
- python-3.5.2

## How it works

1. Data is fetched from the HDFS using the data_loader.py class, taking into account the specified file format (CSV or Parquet).
2. The loaded data is then passed to the query_executor.py class, which executes the designated query depending on the user's choice of data structure (RDD or DataFrame).
3. The execution time for each query is measured and recorded, providing insights into the performance differences between RDD and DataFrame data structures.
4. The system presents the results, allowing users to analyze the performance impact of different data structures and file formats.

## Usage
Step 1 - save your dataset (.tar) in "./datasets".

Step 2 - Define necessary schemas for each of the csv's in schemas.py and update schema_map dict (Key should be the name of each csv).

Step 3 - Run the following command to create the necessary folders in HDFS. <your_dataset_name> should be the name of .tar file in "./datasets" without the extention.
```bash
spark-submit benchmark.py -f hdfs_setup -data <your_dataset_name>
```

Step 4 - Store your dataset in HDFS.
```bash
spark-submit benchmark.py -f save_csv -data <your_dataset_name>
```

Step 5 - Convert and store your dataset as parquet in HDFS.
```bash
spark-submit benchmark.py -f save_parquet -data <your_dataset_name>
```

Step 6 - Update query_data_map.json accordingly (key is the query index and value is a list with the names of the csv's that the query needs.)

Step 7 - Define a new function for your query in query_executor.py. If you want your query to be available for both RDD and DataFrame you should define a new function in each of the classes RddQueryExecutor and DfQueryExecutor (update self.transform_map accordingly).

Step 8 - (OPTIONAL) Define your custom printing function in printer.py. Do so for both RddPrinter and DfPrinter (update self.printer_map accordingly).

Step 9 - Run your query
```bash
spark-submit benchmark.py -f query -file csv -struct rdd -idx_q <index_query> -data <your_dataset_name> -v 1
```
