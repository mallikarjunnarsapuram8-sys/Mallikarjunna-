import os
import sys
print("Testing imports...")
try:
    from pyspark.sql import SparkSession
    print("PySpark imported successfully")
except Exception as e:
    print(f"Failed to import PySpark: {e}")
    sys.exit(1)

print("Testing Spark session...")
try:
    # Set Hadoop home if needed
    if not os.environ.get('HADOOP_HOME'):
        # Try to set it to a common location
        hadoop_path = r"C:\hadoop"
        if os.path.exists(hadoop_path):
            os.environ['HADOOP_HOME'] = hadoop_path
            print(f"Set HADOOP_HOME to {hadoop_path}")
        else:
            print("Warning: HADOOP_HOME not set and C:\\hadoop not found")

    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .getOrCreate()
    print(f"Spark session created successfully: {spark.version}")
    spark.stop()
except Exception as e:
    print(f"Failed to create Spark session: {e}")
    import traceback
    traceback.print_exc()