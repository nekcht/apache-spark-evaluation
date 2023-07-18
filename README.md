# Spark Data Structure Performance Evaluator

This is a simple Python-based system designed, for educational purposes, to evaluate the execution time differences between RDD (Resilient Distributed Datasets) and DataFrame data structures in Apache Spark. It also takes into account the file format being used, such as CSV or Parquet.

## Requirements

- OpenJDK 8
- Apache Hadoop 2.7.7
- Apache Spark 2.4.4
- Python 3.5.2

## How it works

1. Data is fetched from the Hadoop Distributed File System (HDFS) using the data_loader.py class, considering the specified file format (CSV or Parquet).
2. The loaded data is passed to the query_executor.py class, which executes the designated query based on the user's choice of data structure (RDD or DataFrame).
3. The system measures and records the execution time for each query, providing insights into the performance differences between RDD and DataFrame data structures.
4. The results are presented, allowing users to analyze the performance impact of different data structures and file formats.

This implementation includes 5 pre-defined queries for this dataset: https://www.dropbox.com/s/c10t67glk60wpha/datasets2023.tar.gz?dl=0. The queries can be found in query_executor.py. To test them, download the .tar file from the url and store it in /datasets/my_dataset.tar. Then proceed to Steps 3->4->5->9.

## Usage
Step 1 - Save your dataset (.tar) in the ./datasets directory.

Step 2 - Define the necessary schemas for each CSV file in schemas.py and update the schema_map dictionary (the key should be the name of each CSV file).

Step 3 - Run the following command to create the necessary folders in HDFS. Replace <your_dataset_name> with the name of the .tar file in ./datasets without the extension.
```bash
spark-submit benchmark.py -f hdfs_setup -data <your_dataset_name>
```

Step 4 - Store your dataset in HDFS.
```bash
spark-submit benchmark.py -f save_csv -data <your_dataset_name>
```

Step 5 - Convert and store your dataset as Parquet in HDFS.
```bash
spark-submit benchmark.py -f save_parquet -data <your_dataset_name>
```

Step 6 - Update query_data_map.json accordingly. The key is the query index, and the value is a list with the names of the CSV files that the query needs.

Step 7 - Define a new function for your query in query_executor.py. If you want your query to be available for both RDD and DataFrame, define a new function in both the RddQueryExecutor and DfQueryExecutor classes (update self.transform_map accordingly).

Step 8 - (OPTIONAL) Define your custom printing function in printer.py. Do so for both RddPrinter and DfPrinter (update self.printer_map accordingly).

Step 9 - Run your query (file_format = csv or parquet) (data_struct = rdd or df)
```bash
spark-submit benchmark.py -f query -file <file_format> -struct <data_struct> -idx_q <index_query> -data <your_dataset_name> -v 1
```

<file_format> (str): The file format of the data (CSV or Parquet).

<data_struct> (str): The data structure to use (RDD or DataFrame).

<index_query> (int): The index of the query to execute.

<your_dataset_name> (str): The name of the dataset stored in /datasets (e.g. 'my_dataset').

verbose (int): Whether to print query results (1 for True, 0 for False).
