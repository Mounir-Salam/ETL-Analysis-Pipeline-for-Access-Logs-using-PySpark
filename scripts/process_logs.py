from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, to_date, date_trunc
from pyspark.sql.types import TimestampType
from tools.log_Formats import logFormats
from tools.spark_utils import create_spark_session
import argparse
import pandas as pd

import os
import sys

# Use the current Python interpreter inside the venv
os.environ["PYSPARK_PYTHON"] = os.getenv("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv("PYSPARK_DRIVER_PYTHON", sys.executable)

def create_log_table(log_data, log_format):
    """
    Create a table from the log data.
    """
    log_pattern = logFormats.get_format(log_format)

    log_df = log_data.select(
        regexp_extract("value", log_pattern, 1).alias("ip"),
        regexp_extract("value", log_pattern, 2).alias("timestamp"),
        regexp_extract("value", log_pattern, 3).alias("method"),
        regexp_extract("value", log_pattern, 4).alias("endpoint"),
        regexp_extract("value", log_pattern, 5).alias("protocol"),
        regexp_extract("value", log_pattern, 6).alias("status_code"),
        regexp_extract("value", log_pattern, 7).alias("response_time"),
        regexp_extract("value", log_pattern, 8).alias("referer"),
        regexp_extract("value", log_pattern, 9).alias("user_agent")
    )

    return log_df

def clean_log_data(log_df):
    """
    Clean the log data
    Handle data types
    Handle missing values and invalid entries.
    Convert timestamp to a proper format.
    """

    # Replace "-" with None for missing values
    log_df = log_df.na.replace("-", None)

    # Referer can be empty
    log_df = log_df.na.fill("None", subset=["referer"])

    # Convert timestamp to a proper format
    # Assuming the timestamp format is "dd/MMM/yyyy:HH:mm:ss Z"
    # assign a date column
    log_df = log_df.withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z")) \
                    .withColumn("date", to_date(col("timestamp")).cast("string")) \
    # log_df = log_df.withColumn("timestamp", log_df["timestamp"].cast(TimestampType()))
    
    # Convert status_code and response_time to integers
    log_df = log_df.withColumn("status_code", col("status_code").cast("integer")) \
                     .withColumn("response_time", col("response_time").cast("integer"))
    
    log_df.printSchema()
    return log_df

def main(path = "data/access_logs/sample.log",
         output_path = "output/",
         log_format = "combined",
         csv_output = True,
         parquet_output = False,
         mongo_output = False):

    # Initialize Spark session
    spark = create_spark_session(mongo_output = mongo_output)
    spark.sparkContext.setLogLevel("WARN")

    # Load the log data
    log_data = spark.read.text(path)

    # Create the log table
    log_table = create_log_table(log_data, log_format)

    # Clean the log data
    log_table = clean_log_data(log_table)

    # Write the log table to CSV format
    if csv_output:
        csv_output_path = os.path.join(output_path, "data_csv")
        log_table.write.mode("overwrite").csv(csv_output_path, header=True)

    # Write the log table to Parquet format
    if parquet_output:
        parquet_output_path = os.path.join(output_path, "data_parquet")
        log_table.write.mode("overwrite").parquet(parquet_output_path)

    # Write the log table to MongoDB
    if mongo_output:
        collection_name = os.getenv("MONGODB_COLLECTION", "access_logs")
        log_table.write.format("mongo").option("collection", collection_name).mode("append").save()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process log files and create a clean table, and optionally save to MongoDB.")
    parser.add_argument("--path", type=str, default="data/access_logs/sample.log", help="Path to the log file.")
    parser.add_argument("--output_path", type=str, default="output/", help="Path to save the cleaned log data.")
    parser.add_argument("--log_format", type=str, default="combined", choices=["combined", "common"], help="Log format to use for parsing the logs.")
    parser.add_argument("--no_csv_output", action="store_true", help="Disable CSV output (default: enabled)")
    parser.add_argument("--parquet_output", action="store_true", help="Enable Parquet output (default: disabled)")
    parser.add_argument("--mongo_output", action="store_true", help="Enable MongoDB output (default: disabled)")
    args = parser.parse_args()

    main(
        path=args.path,
        output_path=args.output_path,
        log_format=args.log_format,
        csv_output= not args.no_csv_output,
        parquet_output=args.parquet_output,
        mongo_output=args.mongo_output
    )
