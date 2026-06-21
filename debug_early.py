import os
import sys

print("=== DEBUG: At very start of script ===")
print(f"Current directory: {os.getcwd()}")
print(f"JAVA_HOME in env: {os.environ.get('JAVA_HOME', 'NOT SET')}")
print(f"HADOOP_HOME in env: {os.environ.get('HADOOP_HOME', 'NOT SET')}")

# Try to set JAVA_HOME early
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
if os.path.exists(java_path):
    os.environ['JAVA_HOME'] = java_path
    print(f"Set JAVA_HOME to: {java_path}")
else:
    print(f"WARNING: Java path not found: {java_path}")

print(f"After setting - JAVA_HOME in env: {os.environ.get('JAVA_HOME', 'NOT SET')}")

print("\n=== About to import pyspark ===")
try:
    from pyspark.sql import SparkSession
    print("=== PySpark import successful ===")
except Exception as e:
    print(f"=== PySpark import failed: {e} ===")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n=== About to create Spark session ===")
try:
    spark = SparkSession.builder \
        .appName("DebugEarly") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    print(f"=== Spark session created: {spark.version} ===")
except Exception as e:
    print(f"=== Spark session creation failed: {e} ===")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n=== All early tests passed ===")