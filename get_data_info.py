from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("DataInfo") \
        .getOrCreate()
        
    df = spark.read.csv("data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv", header=True, inferSchema=True)
    
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    
if __name__ == "__main__":
    main()
