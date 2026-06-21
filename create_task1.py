import nbformat as nbf
import os

nb = nbf.v4.new_notebook()

# Section 1
md1 = """# Section 1: Objective
**Goal:** The objective of this analysis is to predict whether a flight will arrive 15 minutes or more late (ArrDel15).
This is a **binary classification problem** where the target variable is 1 (delayed) or 0 (not delayed).
"""

# Section 2
cd2 = """from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FlightDelayPrediction_Task1")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)

spark.sparkContext.getConf().getAll()
"""

# Section 3
cd3 = """DATA_PATH = "../data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"

df = spark.read.csv(
    DATA_PATH,
    header=True,
    inferSchema=True
)
"""

# Section 4
cd4 = """# Initial Exploration
df.show(20)
df.printSchema()

# Cache to avoid repeated scans
from pyspark.storagelevel import StorageLevel
df = df.persist(StorageLevel.MEMORY_AND_DISK)

total_rows = df.count()
print(f"Total Rows: {total_rows}")

total_columns = len(df.columns)
print(f"Total Columns: {total_columns}")

print(f"Number of Partitions: {df.rdd.getNumPartitions()}")

import os
file_size_bytes = os.path.getsize(DATA_PATH)
file_size_mb = file_size_bytes / (1024 * 1024)
print(f"File Size: {file_size_mb:.2f} MB")
"""

# Section 5
md5 = """# Section 5: Dataset Validation
* Is the dataset > 10 million rows? (Check output above)
* Is it > 10 columns? Yes, it has many columns.
* Is the dataset from Kaggle? No.
* Is it an official BTS source? Yes, Bureau of Transportation Statistics.
"""

# Section 6
md6 = """# Section 6: Five Vs of Big Data

| V | Description | Evidence |
|---|---|---|
| **Volume** | The massive scale of data. | The dataset is hundreds of megabytes/gigabytes and contains millions of rows representing US flight performance. |
| **Velocity** | The speed at which data is generated. | Flights occur continuously, and status updates (delays, arrivals) are reported in real-time or near real-time across the US. |
| **Variety** | The different forms of data. | The dataset includes numerical (times, distances), categorical (carrier codes, airports), and geospatial data. |
| **Veracity** | The uncertainty or reliability of data. | Since it is from an official government source (BTS), veracity is high, but we still handle missing or anomalous values. |
| **Value** | The usefulness of the data. | Predicting flight delays has immense value for airlines (optimizing routes) and passengers (saving time). |
"""

# Section 7
md7 = """# Section 7: Ethics and Licensing
* **Public Availability:** This dataset is publicly available from the US Department of Transportation.
* **Privacy Considerations:** The data does not contain personally identifiable information (PII) of passengers, making privacy risks minimal.
* **Potential Bias:** Delay patterns might reflect systemic issues at certain regional airports rather than carrier performance alone. Models must be interpreted carefully to not unfairly penalize specific entities without context.
* **Responsible Use:** Predictive models should be used to improve operational efficiency and passenger experience, not for anti-competitive practices.
"""

# Section 8
md8 = """# Section 8: Screenshot Checklist
- [ ] df.show(20)
- [ ] printSchema()
- [ ] record count
- [ ] file size
- [ ] partition count
"""

# Section 9
md9 = """# Section 9: Answer Sheet Text

**Problem Definition**
The goal is to develop a binary classification model predicting whether a flight will be delayed by 15 minutes or more (`ArrDel15`). This helps airlines improve operational efficiency and enhances the passenger experience by providing proactive delay estimations.

**Dataset Description**
The dataset used is the official US Airline On-Time Performance data for 2024 from the Bureau of Transportation Statistics. It contains millions of records detailing flight schedules, actual departure/arrival times, and carrier information.

**Five Vs of Big Data**
See the table in Section 6. The data exemplifies Volume (millions of records), Velocity (continuous flight operations), Variety (mixed data types), Veracity (official BTS source), and Value (predictive insights).

**Ethics and Licensing**
The data is publicly licensed and lacks PII, ensuring passenger privacy. However, we must ensure responsible use by not misinterpreting regional airport delays as inherent carrier flaws, mitigating potential bias in operational decisions.
"""

cells = [
    nbf.v4.new_markdown_cell(md1),
    nbf.v4.new_code_cell(cd2),
    nbf.v4.new_code_cell(cd3),
    nbf.v4.new_code_cell(cd4),
    nbf.v4.new_markdown_cell(md5),
    nbf.v4.new_markdown_cell(md6),
    nbf.v4.new_markdown_cell(md7),
    nbf.v4.new_markdown_cell(md8),
    nbf.v4.new_markdown_cell(md9)
]

nb['cells'] = cells

os.makedirs('notebooks', exist_ok=True)
with open('notebooks/Task1_Data_Understanding.ipynb', 'w') as f:
    nbf.write(nb, f)
