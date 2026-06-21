import os
import sys
import subprocess

# Set JAVA_HOME to the correct Java installation
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ['JAVA_HOME'] = java_path
print(f"Set JAVA_HOME to: {java_path}")

# Also set HADOOP_HOME if we have it
hadoop_path = r"C:\hadoop"
if os.path.exists(hadoop_path):
    os.environ['HADOOP_HOME'] = hadoop_path
    print(f"Set HADOOP_HOME to: {hadoop_path}")
else:
    print(f"Warning: Hadoop path not found at {hadoop_path}")
    print("Please download winutils.exe and place in C:\\hadoop\\bin\\")

# Set PySpark Python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("\nTesting Spark session with correct JAVA_HOME...")
try:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    print(f"Spark session created successfully! Version: {spark.version}")

    # Test with a simple DataFrame
    df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
    df.show()

    spark.stop()
    print("Test completed successfully!")

except Exception as e:
    print(f"Failed to create Spark session: {e}")
    import traceback
    traceback.print_exc()

    # Try to run spark-submit directly to see if it works now
    print("\nTrying spark-submit directly...")
    try:
        spark_submit = r"C:\Users\shiva\AppData\Roaming\Python\Python312\site-packages\pyspark\bin\spark-submit.cmd"
        if os.path.exists(spark_submit):
            result = subprocess.run([spark_submit, "--version"],
                                  capture_output=True, text=True, timeout=30)
            print(f"spark-submit return code: {result.returncode}")
            if result.stdout:
                print(f"stdout: {result.stdout}")
            if result.stderr:
                print(f"stderr: {result.stderr}")
        else:
            print(f"spark-submit not found at {spark_submit}")
    except Exception as e2:
        print(f"spark-submit also failed: {e2}")