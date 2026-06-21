import os

DATA_DIR = r"C:\Users\shiva\Downloads\On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1"
DATA_FILE = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"
DATA_PATH = os.path.join(DATA_DIR, DATA_FILE)

print(f"DATA_DIR: {DATA_DIR}")
print(f"DATA_FILE: {DATA_FILE}")
print(f"DATA_PATH: {DATA_PATH}")

print(f"os.path.exists(DATA_DIR): {os.path.exists(DATA_DIR)}")
print(f"os.path.exists(DATA_PATH): {os.path.exists(DATA_PATH)}")

if os.path.exists(DATA_PATH):
    print("File exists and is accessible")
    # Try to get file size
    try:
        size = os.path.getsize(DATA_PATH)
        print(f"File size: {size} bytes")
    except Exception as e:
        print(f"Error getting file size: {e}")
else:
    print("File does not exist or is not accessible")