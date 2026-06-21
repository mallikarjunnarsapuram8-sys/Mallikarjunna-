import os
import sys
import traceback

print("Python executable:", sys.executable)
print("Trying to import pyspark...")
try:
    import pyspark
    print("PySpark imported successfully")
except Exception as e:
    print(f"Failed to import PySpark: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\nSetting PySpark Python environment variables...")
# Explicitly set the Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
print(f"PYSPARK_PYTHON set to: {os.environ['PYSPARK_PYTHON']}")
print(f"PYSPARK_DRIVER_PYTHON set to: {os.environ['PYSPARK_DRIVER_PYTHON']}")

print("\nTrying to create SparkSession...")
try:
    from pyspark.sql import SparkSession

    # Try to create SparkSession with basic config
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    print(f"SparkSession created successfully!")
    print(f"Spark version: {spark.version}")

    # Try a simple operation
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    print("Created DataFrame:")
    df.show()

    spark.stop()
    print("SparkSession stopped successfully")

except Exception as e:
    print(f"Failed to create or use SparkSession: {e}")
    print("Full traceback:")
    traceback.print_exc()