import os
import sys
import traceback

print("Python version:", sys.version)
print("Trying to import pyspark...")
try:
    import pyspark
    print("PySpark imported successfully")
    print("PySpark version:", pyspark.__version__)
except Exception as e:
    print(f"Failed to import PySpark: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\nTrying to create SparkSession...")
try:
    from pyspark.sql import SparkSession

    # Set environment variables that might help
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Try to create SparkSession with basic config
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    print(f"SparkSession created successfully!")
    print(f"Spark version: {spark.version}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Master: {spark.sparkContext.master}")

    # Try a simple operation
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    df.show()

    spark.stop()
    print("SparkSession stopped successfully")

except Exception as e:
    print(f"Failed to create or use SparkSession: {e}")
    print("Full traceback:")
    traceback.print_exc()

    # Provide troubleshooting hints
    print("\n" + "="*50)
    print("TROUBLESHOOTHING HINTS:")
    print("="*50)
    print("1. Hadoop winutils.exe missing:")
    print("   - Download from: https://github.com/steveloughran/winutils")
    print("   - Place in C:\\hadoop\\bin\\winutils.exe")
    print("   - Set HADOOP_HOME=C:\\hadoop")
    print("")
    print("2. Java not found or wrong version:")
    print("   - Run 'java -version' to check")
    print("   - Spark requires Java 8 or 11")
    print("")
    print("3. Permission issues:")
    print("   - Check permissions on temp directories")
    print("   - Try running as administrator")
    print("")
    print("4. Spark configuration conflicts:")
    print("   - Check for existing SPARK_HOME or conflicting configs")
    print("="*50)