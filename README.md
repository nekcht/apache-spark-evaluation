# Spark Data Structure Performance Evaluator

This is a simple Python-based system designed, for educational purposes, to evaluate the execution time differences between RDD (Resilient Distributed Datasets) and DataFrame data structures in Apache Spark. It also takes into account the file format being used, such as CSV or Parquet.

## Requirements

- OpenJDK 8
- Apache Hadoop 2.7.7
- Apache Spark 2.4.4
- Python 3.5.2

## Description
When you try to execute a query, the system typically works like this:
1. Data is fetched from the Hadoop Distributed File System (HDFS) using the data_loader.py class, considering the specified file format (CSV or Parquet).
2. The loaded data is passed to the query_executor.py class, which executes the designated query based on the user's choice of data structure (RDD or DataFrame).
3. The system measures and records the execution time for each query, providing insights into the performance differences between RDD and DataFrame data structures.

This implementation includes 5 pre-defined queries for this dataset: https://www.dropbox.com/s/c10t67glk60wpha/datasets2023.tar.gz?dl=0. The queries can be found in query_executor.py. To test them, download the .tar file from the url and store it in '/datasets/my_dataset.tar'. Then proceed directly to Steps 3 -> 4 -> 5 -> 9.

## How to
* How to define my own queries (transformations) on a new dataset?
> Compress your csv's (.tar file) and store it in '/datasets' dir. Then, define your own transformations function in query_executor.py.
If you want to use both RDD and DataFrame structures you must define a function in each subclass of QueryExecutor. Also, don't forget
to update the 'transform_map' dictionary in QueryExecutor class accordingly. The default implementation contains 5 queries defined as
> 'query_1', 'query_2'...etc. This means that you first new query would be 'def query_6()'. Lastly, update query_data_map.json accordingly.

* How to use a new dataset for my queries?
> As mentioned, compress your csv's (.tar file) and store it in '/datasets' dir. Then, just proceed to Steps 3 -> 4 -> 5.

* How to define my custom printing function?
> Define your own printing function in printer.py. As before, if you want to define different printing functions for RDD and DF, you should
do so in both subclasses of Printer class. The default implementation contains one printing function for each query as 'print_query_1',
'print_query_2'...etc. Also, update the attribute 'printer_map' in Printer class accordingly.

* How to define my own schemas for my new dataset?
> Define your own schemas in schemas.py. Update schema_map dictionary accordingly.


## Usage
Step 1 - Save your dataset (.tar) in the ./datasets directory.

Step 2 - Define the necessary schemas for each CSV file in schemas.py and update the schema_map dictionary (the key should be the name of each CSV file).

Step 3 - Run the following command to create the necessary folders in HDFS. Replace <your_dataset_name> with the name of the .tar file in ./datasets without the extension.
```bash
spark-submit benchmark.py -f hdfs_setup -data <your_dataset_name>
```

Step 4 - Store your dataset in HDFS:
```bash
spark-submit benchmark.py -f save_csv -data <your_dataset_name>
```

Step 5 - Convert and store your dataset as Parquet in HDFS:
```bash
spark-submit benchmark.py -f save_parquet -data <your_dataset_name>
```

Step 6 - Update query_data_map.json accordingly. The key is the query index, and the value is a list with the names of the CSV files that the query needs.

Step 7 - Define a new function for your query in query_executor.py. If you want your query to be available for both RDD and DataFrame, define a new function in both the RddQueryExecutor and DfQueryExecutor classes (update self.transform_map accordingly).

Step 8 - (OPTIONAL) Define your custom printing function in printer.py. Do so for both RddPrinter and DfPrinter. Update self.printer_map accordingly.

Step 9 - Run your query:
```bash
spark-submit benchmark.py -f query -file <file_format> -struct <data_struct> -idx_q <index_query> -data <your_dataset_name> -v 1
```
If you wish to save the results in a .txt file in '/output' use:
```bash
spark-submit benchmark.py -f query -file <file_format> -struct <data_struct> -idx_q <index_query> -data <your_dataset_name> -v 1 > ../output/result.txt
```
- <file_format> (str): The file format of the data (CSV or Parquet).

- <data_struct> (str): The data structure to use (RDD or DataFrame).

- <index_query> (int): The index of the query to execute.

- <your_dataset_name> (str): The name of the dataset stored in /datasets (e.g. 'my_dataset').
