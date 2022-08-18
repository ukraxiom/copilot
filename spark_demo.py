from datetime import datetime, timedelta
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import sys


def main():
    sparksession = SparkSession.builder.appName("Spark Demo").getOrCreate()
    logging.info("Spark Session created")
    sparksession.conf.set("spark.sql.caseSensitive", "true")
    sparksession.conf.set(
        "spark.sql.soources.partitionOverwriteMode", "dynamic")
    logging.info("Spark Session configuration set")

    # Read the data from the file
    df = sparksession.read.csv(sys.argv[1], header=True, inferSchema=True)
    logging.info("Dataframe created")

    # Add a new column to the dataframe
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    logging.info("Date column added")
    # add new column with month value from date column
    df = df.withColumn("month", to_date(col("date"), "yyyy-MM-dd").month)

    # Print the data
    df.show()

    # function to parse apache log format
    def parse_apache_log(line):
        # Split the line into words
        words = line.split()
        # Extract the fields from the original line
        ip = words[0]
        client = words[2]
        user = words[3]
        date = words[4]
        request = words[5]
        status = words[6]
        size = words[7]
        # Parse the date into a Python datetime object
        dt = datetime.strptime(date, "%d/%b/%Y:%H:%M:%S")
        # Return a tuple of the original line and the datetime object
        return (line, dt)

    # Parse the log lines
    parsed_logs = df.rdd.map(parse_apache_log)
    logging.info("Parsed logs created")
    # Convert the RDD to a DataFrame
    parsed_logs_df = sparksession.createDataFrame(
        parsed_logs, ["line", "date"])
    logging.info("Parsed logs dataframe created")
    # Print the data
    parsed_logs_df.show()

    # code by codewhisper

    # function to read data from excel file

    def read_excel(path):
        # Read the data from the file
        df = sparksession.read.excel(path, header=True, inferSchema=True)
        logging.info("Dataframe created")
        # Add a new column to the dataframe
        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        logging.info("Date column added")
        # add new column with month value from date column
        df = df.withColumn("month", to_date(col("date"), "yyyy-MM-dd").month)
        logging.info("Date column added")
        # Print the data
        df.show()
        return df
    
    
# code by copilot

# function to read data from excel file

    def read_excel(path):
        # Read the data from the file
        df = sparksession.read.excel(path, header=True, inferSchema=True)
        logging.info("Dataframe created")
        # Add a new column to the dataframe
        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        logging.info("Date column added")
        # add new column with month value from date column
        df = df.withColumn("month", to_date(col("date"), "yyyy-MM-dd").month)
        logging.info("Date column added")
        # Print the data
        df.show()
        return df

    # Parse the log lines
    parsed_logs = df.rdd.map(parse_apache_log)
    logging.info("Parsed logs created")
    # Convert the RDD to a DataFrame
    parsed_logs_df = sparksession.createDataFrame(
        parsed_logs, ["line", "date"])
    logging.info("Parsed logs dataframe created")
    # Print the data
    parsed_logs_df.show()

# function to check if json file is valid
def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True




