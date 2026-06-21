import os
import sys

print("=== Debugging Main Script ===")
print(f"Python executable: {sys.executable}")

# Set environment variables
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ['JAVA_HOME'] = java_path
print(f"Set JAVA_HOME to: {java_path}")

hadoop_path = r"C:\hadoop"
if os.path.exists(hadoop_path):
    os.environ['HADOOP_HOME'] = hadoop_path
    print(f"Set HADOOP_HOME to: {hadoop_path}")
else:
    print(f"Warning: Hadoop path not found at {hadoop_path}")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("\n=== Testing Paths ===")
DATA_DIR = r"C:\Users\shiva\Downloads\On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1"
DATA_FILE = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"
DATA_PATH = os.path.join(DATA_DIR, DATA_FILE)

print(f"DATA_DIR: {DATA_DIR}")
print(f"DATA_PATH: {DATA_PATH}")
print(f"Exists: {os.path.exists(DATA_PATH)}")

print("\n=== Testing Imports ===")
try:
    from pyspark.sql import SparkSession
    print("PySpark imports successful")
except Exception as e:
    print(f"PySpark import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n=== Creating Spark Session ===")
try:
    spark = SparkSession.builder \
        .appName("Debug") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    print(f"Spark session created: {spark.version}")
except Exception as e:
    print(f"Spark session creation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n=== Testing File Read ===")
try:
    print(f"Attempting to read: {DATA_PATH}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("nullValue", "") \
        .option("treatEmptyValuesAsNulls", "true") \
        .csv(DATA_PATH)
    print(f"Successfully read data! Count: {df.count()}")
    df.show(5)
except Exception as e:
    print(f"Failed to read data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n=== All Tests Passed ===")
spark.stop()