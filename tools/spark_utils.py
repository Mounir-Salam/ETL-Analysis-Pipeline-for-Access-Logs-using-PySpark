import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Ensure environment variable is set (adjust path if needed)
os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
os.environ["PATH"] += os.pathsep + os.getenv("HADOOP_BIN", "C:\\hadoop\\bin")

def create_spark_session(app_name="LogProcessing", mongo_output=False):
    load_dotenv()

    if mongo_output:
        # Get the MongoDB connection details
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        mongo_db = os.getenv("MONGODB_DATABASE", "logs_db")

        # Optional setting for tidy cleanup .config("spark.local.dir", "C:/Temp/spark-temp")
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.3") \
            .config("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db}") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        
    return spark