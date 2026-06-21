import os
import sys
from pyspark.sql import SparkSession

# Set environment variables for Hadoop
os.environ['HADOOP_HOME'] = r'C:\hadoop'

# Create SparkSession with explicit configurations
spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", r"C:\tmp\spark-warehouse") \
    .config("spark.local.dir", r"C:\tmp\spark-local") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("Spark version:", spark.version)
spark.stop()
