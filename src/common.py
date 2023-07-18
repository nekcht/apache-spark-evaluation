# common.py
# Christou Nektarios - Big Data Management 2022-2023 NKUA


"""
Auxiliary functions
"""


# standard library imports
import json
import os
import subprocess
import json
from pathlib import Path
import tarfile
# local module imports
from schemas import get_schema


def get_json(json_file):
    """
    Reads a JSON file and returns its contents as a dictionary.

    Args:
        json_file (str): The name of the JSON file.

    Returns:
        dict: The contents of the JSON file as a dictionary.
    """

    # Read the JSON file
    with open('{}.json'.format(json_file)) as file:
        my_dict = json.load(file)

    return my_dict


def update_json(json_file, entry_tuple):
    """
    Updates a JSON file by adding a new entry.

    Args:
        json_file (str): The name of the JSON file to update.
        entry_tuple (tuple): A tuple containing the new entry (key, value).
    """

    key = entry_tuple[0]
    value = entry_tuple[1]

    with open(json_file, 'r') as file:
        data = json.load(file)

    # Check if new entry already exists in the JSON data
    if key in data:
        print("Entry already exists. Update failed. If this persists reset .json files to their original state.")
        return

    # Append the new entry to the JSON data
    data[key] = value

    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)


def hdfs_setup(dataset):
    """
    Creates all necessary folders in HDFS for a specific dataset.

    Args:
        dataset (str): The name of the dataset file in the '../datasets' directory.
    """

    # get configuration
    conf = get_json('paths')
    datasets_path = conf['hdfs_datasets_path']

    # set paths
    dataset_dir = os.path.join(datasets_path, dataset)
    dataset_csv_dir = os.path.join(dataset_dir, 'csv')
    dataset_parquet_dir = os.path.join(dataset_dir, 'parquet')

    # create hdfs folders
    subprocess.call("hadoop fs -mkdir -p {}".format(dataset_dir), shell=True)
    subprocess.call("hadoop fs -mkdir -p {}".format(dataset_csv_dir), shell=True)
    subprocess.call("hadoop fs -mkdir -p {}".format(dataset_parquet_dir), shell=True)

    # update conf.json
    new_entry = (dataset, dataset_dir)
    update_json('paths.json', new_entry)

    return None


def save_csv(dataset):
    """
    Extracts the dataset specified in '../datasets' and stores all CSV files in HDFS.

    Args:
        dataset (str): The name of the dataset file that contains the CSV files (e.g., dataset2023).
    """

    # get current dir
    current_directory = os.getcwd()

    # get configuration
    conf = get_json('paths')
    dataset_local_dir = conf['local_datasets_path']
    dataset_dir = conf[dataset]
    dataset_csv_dir = os.path.join(dataset_dir, 'csv')

    # Change current working directory to dataset_local_dir
    os.chdir(dataset_local_dir)

    # find & unzip dataset
    file_exists = False
    for file in os.listdir('.'):
        file_without_extension  = os.path.splitext(file)[0]
        if file_without_extension == dataset:
            # create a new folder
            os.makedirs(dataset, exist_ok=True)
            # Extract the contents of the dataset into the new folder
            with tarfile.open(file, 'r') as tar:
                tar.extractall(dataset)
            file_exists = True
            break
    if not file_exists:
        print("The file does not exist in the current directory.")

    # Change current working directory to the new folder we created
    os.chdir(dataset)

    csv_names = []
    # store all csv's to HDFS
    for file in os.listdir('.'):
        file_without_extension = os.path.splitext(file)[0]
        if file.endswith(".csv"):
            print("\tSTORING {} IN HDFS".format(file))
            subprocess.call("hadoop fs -put {} {}".format(file, dataset_csv_dir), shell=True)

            # add the name of the csv to a list in order to update
            # datasets_map.json later on.
            csv_names.append(file_without_extension)

    # Change current working directory to dataset_local_dir
    os.chdir('..')

    # clean extracted files
    subprocess.call("rm -r {}".format(dataset), shell=True)

    # update datasets_map.json
    os.chdir('../src')
    new_entry = {
        dataset: csv_names
    }
    with open('datasets_map.json', 'w') as file:
        json.dump(new_entry, file, indent=4)


    return None


def save_parquet(spark, dataset):
    """
    Converts the input dataset to Parquet format and stores it in HDFS.

    Args:
        spark (pyspark.sql.SparkSession): The SparkSession object.
        dataset (str): The name of the dataset (specified in datasets_map.json).
    """

    # get configuration
    conf = get_json('paths')
    hdfs_datasets = conf['hdfs_datasets_path']
    dataset_dir = os.path.join(hdfs_datasets, dataset)
    dataset_csv_dir = os.path.join(dataset_dir, 'csv')
    dataset_parquet_dir = os.path.join(dataset_dir, 'parquet')

    # find all csv files for this dataset
    dataset_map = get_json('datasets_map')
    csv_list = dataset_map[dataset]

    # convert all csv's to parquet and store to HDFS
    for csv_file in csv_list:
        # get schema
        schema = get_schema(csv_file)

        # read data
        df = spark.read.format('csv') \
            .schema(schema) \
            .load(os.path.join(dataset_csv_dir, csv_file + '.csv'))

        # write as parquet
        print("\tSTORING {} (parquet) IN HDFS".format(csv_file))
        df.write.parquet(os.path.join(dataset_parquet_dir, csv_file))

    return None


def get_execution_times():
    """
    Extracts the execution times from the output files and prints them.

    This function assumes that the output files are located in the '../output'
    directory and have the '.txt' extension.
    """
    import os

    # Change current working directory to dataset_local_dir
    os.chdir('../output')


    execution_times = {}
    # Iterate over each file in the directory
    for filename in os.listdir('.'):
        if filename.endswith('.txt'):  # Only consider text files
            with open(filename, 'r') as file:
                # Read each line in the file
                for line in file:
                    if line.startswith('Execution time:'):
                        # Extract the execution time from the line
                        execution_time = float(line.split(':')[1])
                        execution_times[os.path.splitext(filename)[0]] = execution_time

    # Print the extracted execution times
    for filename, time in execution_times.items():
        print("{}: {}".format(filename, time))
