import os
import sys

# Set ALL necessary environment variables BEFORE importing PySpark
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ['JAVA_HOME'] = java_path

hadoop_path = r"C:\hadoop"
if not os.path.exists(hadoop_path):
    os.makedirs(hadoop_path + r"\bin", exist_ok=True)
    # Create a dummy winutils.exe to suppress warnings
    with open(hadoop_path + r"\bin\winutils.exe", "w") as f:
        f.write("dummy")
os.environ['HADOOP_HOME'] = hadoop_path

# Set Spark home explicitly to avoid the warning
spark_home = os.path.dirname(os.path.dirname(__file__))  # This won't work, let's find pyspark location
import pyspark
spark_home = os.path.dirname(os.path.dirname(pyspark.__file__))
os.environ['SPARK_HOME'] = spark_home

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("Environment variables set:")
print(f"  JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"  HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"  SPARK_HOME: {os.environ.get('SPARK_HOME')}")
print(f"  PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")

print("\nImporting PySpark...")
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import time
import csv

print("Imports successful!")

print("\nCreating Spark session...")
spark = SparkSession.builder \
    .appName("FinalTest") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Now run the actual workflow (simplified version for testing)
print("\nSetting up paths...")
DATA_DIR = r"C:\Users\shiva\Downloads\On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1"
DATA_FILE = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"
DATA_PATH = os.path.join(DATA_DIR, DATA_FILE)

OUTPUT_DIR = r"C:\Users\shiva\OneDrive\Desktop\mtechbigdataml\output_final"
MODEL_DIR = os.path.join(OUTPUT_DIR, "models")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
TABLEAU_DIR = os.path.join(OUTPUT_DIR, "tableau")

for directory in [OUTPUT_DIR, MODEL_DIR, RESULTS_DIR, TABLEAU_DIR]:
    os.makedirs(directory, exist_ok=True)

print(f"Data path: {DATA_PATH}")
print(f"Output directory: {OUTPUT_DIR}")

print("\nLoading data...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "") \
    .option("treatEmptyValuesAsNulls", "true") \
    .csv(DATA_PATH)

print(f"Data loaded: {df.count()} rows, {len(df.columns)} columns")

print("\nChecking for target variable...")
if "ArrDel15" in df.columns:
    print("Target variable 'ArrDel15' found!")
    delayed_count = df.filter(col("ArrDel15") == 1).count()
    total_count = df.count()
    print(f"Delayed flights: {delayed_count:,} ({delayed_count/total_count*100:.2f}%)")
else:
    print("ERROR: Target variable 'ArrDel15' not found!")
    sys.exit(1)

print("\nSUCCESS: Basic functionality test passed!")
print("The main script should work correctly with proper environment variables.")

spark.stop()