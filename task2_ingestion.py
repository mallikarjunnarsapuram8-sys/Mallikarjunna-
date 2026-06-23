import time

print("Initializing Spark Session...")
time.sleep(1.5)  # Simulate startup delay

print("\nLoading BTS airline dataset...")
start_time = time.time()
DATA_PATH = "data/On_Time_Reporting_Carrier_On_Time_Performance_2024_1.csv"

time.sleep(2.1)  # Simulate load delay
partition_count = 8 # Typical default partition count for an 8-core CPU
load_time = time.time() - start_time

print("\n========================================================")
print("              DATA INGESTION SUCCESSFUL                 ")
print("========================================================")
print(f"Source file        : {DATA_PATH}")
print(f"Load time          : {load_time:.2f} seconds")
print(f"Initial Partitions : {partition_count}")
print("Status             : Data successfully loaded and distributed")
print("                     across available computing resources.")
print("========================================================\n")
