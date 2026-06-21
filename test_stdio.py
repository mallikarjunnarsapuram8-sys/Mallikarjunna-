import os
import sys
import subprocess

# Set environment variables
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ['JAVA_HOME'] = java_path
hadoop_path = r"C:\hadoop"
if os.path.exists(hadoop_path):
    os.environ['HADOOP_HOME'] = hadoop_path
else:
    # Create the directory and a dummy winutils.exe for testing
    os.makedirs(hadoop_path + r"\bin", exist_ok=True)
    with open(hadoop_path + r"\bin\winutils.exe", "w") as f:
        f.write("dummy")
    os.environ['HADOOP_HOME'] = hadoop_path

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("=== Testing with subprocess to capture stdout/stderr separately ===")

# Test 1: Simple import and session creation
code = '''
import os
print("JAVA_HOME:", os.environ.get('JAVA_HOME'))
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
from pyspark.sql import SparkSession
print("Import successful")
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
print("Spark created:", spark.version)
spark.stop()
print("Test completed")
'''

try:
    result = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=30)
    print("=== STDOUT ===")
    print(result.stdout)
    if result.stderr:
        print("=== STDERR ===")
        print(result.stderr)
    print(f"Return code: {result.returncode}")
except Exception as e:
    print(f"Subprocess failed: {e}")