import traceback
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('test').getOrCreate()
    print(spark.version)
    spark.stop()
except Exception as e:
    traceback.print_exc()
