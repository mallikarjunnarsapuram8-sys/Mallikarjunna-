import os
import sys
import subprocess

# Set environment variables
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ['JAVA_HOME'] = java_path

hadoop_path = r"C:\hadoop"
if not os.path.exists(hadoop_path):
    os.makedirs(hadoop_path + r"\bin", exist_ok=True)
    with open(hadoop_path + r"\bin\winutils.exe", "w") as f:
        f.write("dummy")
os.environ['HADOOP_HOME'] = hadoop_path

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Let's monkey patch subprocess.Popen to see what command is being run
original_popen = subprocess.Popen

def debug_popen(*args, **kwargs):
    print(f"DEBUG Popen called with args: {args}")
    print(f"DEBUG Popen called with kwargs: {kwargs}")
    if args:
        print(f"First arg (command): {args[0]}")
        if isinstance(args[0], list):
            print(f"Command list: {args[0]}")
            for i, part in enumerate(args[0]):
                print(f"  [{i}]: {part} (exists: {os.path.isfile(part) if os.path.isabs(part) else 'relative/path'})")
    # Call the original function
    return original_popen(*args, **kwargs)

subprocess.Popen = debug_popen

print("Environment variables set:")
print(f"  JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"  HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")

print("\nImporting PySpark...")
try:
    from pyspark.sql import SparkSession
    print("Import successful!")
except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nAttempting to create Spark session...")
try:
    spark = SparkSession.builder \
        .appName("DebugGateway") \
        .master("local[*]") \
        .getOrCreate()
    print("Spark session created successfully!")
    spark.stop()
except Exception as e:
    print(f"Spark session creation failed: {e}")
    import traceback
    traceback.print_exc()