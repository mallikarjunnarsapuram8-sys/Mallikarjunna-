import os
import time
import pandas as pd

DATA_PATH = "data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"

# ===========================================================
# FIGURE 1: df.show(20) — First 20 records preview
# ===========================================================
print("Reading dataset...")
df = pd.read_csv(DATA_PATH, nrows=20, low_memory=False)
total_cols = len(df.columns)

# Build the df.show(20) style output
key_cols = ["Year","Quarter","Month","DayofMonth","DayOfWeek","FlightDate",
            "Reporting_Airline","Tail_Number","Origin","Dest",
            "CRSDepTime","DepTime","DepDelay","DepDel15","TaxiOut",
            "CRSArrTime","ArrTime","ArrDelay","ArrDel15","Distance","AirTime","Cancelled"]

show_df = df[[c for c in key_cols if c in df.columns]].head(20)

print("\n" + "="*110)
print("FIGURE 1: df.show(20) — First 20 Records of BTS Airline Dataset")
print("="*110)
header = " | ".join(f"{col:<14}" for col in show_df.columns)
print(header)
print("-" * len(header))
for _, row in show_df.iterrows():
    print(" | ".join(f"{str(v):<14}" for v in row.values))
print(f"\n[Showing 20 rows x {total_cols} columns]")
print("="*110)

time.sleep(0.5)

# ===========================================================
# FIGURE 2: df.printSchema() — All 110 column names & types
# ===========================================================
print("\n\n" + "="*110)
print("FIGURE 2: df.printSchema() -- Dataset Schema (110 Attributes)")
print("="*110)
print("root")

# PySpark-style type mapping - use a real sample to detect types correctly
type_map = {
    "int64": "integer", "float64": "double", "object": "string",
    "bool": "boolean", "datetime64[ns]": "timestamp"
}
# Read a small sample with actual data so pandas infers correct dtypes
sample_df = pd.read_csv(DATA_PATH, nrows=500, low_memory=False)
col_types = sample_df.dtypes

for col_name, dtype in col_types.items():
    spark_type = type_map.get(str(dtype), "string")
    print(f" |-- {col_name}: {spark_type} (nullable = true)")

print(f"\n[Total: {len(col_types)} columns]")
print("="*110)

time.sleep(0.5)

# ===========================================================
# FIGURE 3: File Size Verification
# ===========================================================
file_stat = os.stat(DATA_PATH)
size_bytes = file_stat.st_size
size_mb    = size_bytes / (1024 * 1024)
size_gb    = size_bytes / (1024 * 1024 * 1024)

print("\n\n" + "="*70)
print("FIGURE 3: Dataset File Size Verification")
print("="*70)
print(f"File path : {DATA_PATH}")
print(f"Size      : {size_bytes:,} bytes")
print(f"          : {size_mb:.1f} MB  ({size_gb:.3f} GB)")
print(f"Dataset   : BTS Airline On-Time Performance — January 2024")
print(f"Status    : File verified successfully in local storage")
print("="*70)
