from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys

# Ensure environment variable is set (adjust path if needed)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

# Use the current Python interpreter inside the venv
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IO Test") \
    .master("local[*]") \
    .getOrCreate()

# Sample data and schema
data = [("Alice", 30), ("Bob", 25), ("Charlie", 40)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
df = spark.createDataFrame(data, schema)

# Paths to test I/O
parquet_path = "test_output/parquet_data"
csv_path = "test_output/csv_data"

# Clean output directories first (Spark throws error if exists)
import shutil
shutil.rmtree(parquet_path, ignore_errors=True)
shutil.rmtree(csv_path, ignore_errors=True)

# Write to Parquet and CSV
try:
    df.write.parquet(parquet_path)
    print("[success] Parquet write successful")
except Exception as e:
    print("[Failure] Parquet write failed:", e)

try:
    df.write.csv(csv_path, header=True)
    print("[success] CSV write successful")
except Exception as e:
    print("[Failure] CSV write failed:", e)

# Read them back
try:
    df_parquet = spark.read.parquet(parquet_path)
    df_parquet.show()
except Exception as e:
    print("[Failure] Parquet read failed:", e)

try:
    df_csv = spark.read.option("header", True).csv(csv_path)
    df_csv.show()
except Exception as e:
    print("[Failure] CSV read failed:", e)

spark.stop()