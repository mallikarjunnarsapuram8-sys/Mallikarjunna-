# 7006SCN – Machine Learning and Big Data
## Distributed Flight Delay Prediction Using PySpark and US Airline On-Time Performance Data

[![Module](https://img.shields.io/badge/Module-7006SCN-blue)](https://www.coventry.ac.uk/)
[![Language](https://img.shields.io/badge/Language-Python%203.12-green)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-PySpark-orange)](https://spark.apache.org/)
[![Dataset](https://img.shields.io/badge/Dataset-BTS%20On--Time%20Performance-lightblue)](https://www.bts.gov/)

---

## Project Overview

This project implements a distributed machine learning pipeline using **Apache Spark (PySpark)** to predict flight arrival delays (≥15 minutes) from the **US Bureau of Transportation Statistics (BTS) On-Time Performance Dataset**.

| Property | Value |
|---|---|
| **Module** | 7006SCN – Machine Learning and Big Data |
| **Dataset** | BTS Airline On-Time Performance Data (January 2024) |
| **Target Variable** | `ArrDel15` (binary: 1 = delayed ≥15 min, 0 = on time) |
| **Dataset Size** | 547,271 rows × 110 columns |
| **File Size** | 235.4 MB |
| **Framework** | PySpark (Apache Spark) |

---

## Repository Structure

```
7006SCN_Initials_SID/
│
├── notebooks/
│   ├── Task1.ipynb                          # Week 1–2: Data Understanding
│   ├── Task2.ipynb                          # Week 2:   Data Engineering
│   ├── Task3.ipynb                          # Week 3:   Model Development
│   ├── Task4.ipynb                          # Week 4:   Distributed Computing
│   ├── Task5.ipynb                          # Week 5:   Evaluation & Stability
│   ├── Task6.ipynb                          # Week 6:   Tableau Export
│   ├── Task1_Data_Understanding.ipynb       # Full Task 1 (detailed version)
│   ├── Task2_Data_Engineering.ipynb         # Full Task 2 (detailed version)
│   ├── Task3_Model_Development.ipynb        # Full Task 3 (detailed version)
│   ├── Task4_Distributed_Computing.ipynb    # Full Task 4 (detailed version)
│   ├── Task5_Evaluation_and_Stability.ipynb # Full Task 5 (detailed version)
│   └── Task6_Tableau_Export.ipynb           # Full Task 6 (detailed version)
│
├── data/
│   └── On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv
│
├── screenshots/
│   ├── confusion_matrices/                  # Task 5 confusion matrix PNGs
│   ├── roc_curves/                          # Task 5 ROC curve PNGs
│   ├── pr_curves/                           # Task 5 Precision-Recall PNGs
│   ├── feature_importance/                  # Task 5 feature importance PNGs
│   └── model_explainability/                # Task 5 SHAP explainability PNGs
│
├── output_final/                            # Final model outputs and results
│   ├── models/                              # Saved PySpark ML models
│   ├── results/                             # Evaluation metrics CSV/JSON
│   └── tableau/                             # Tableau export files
│
├── FlightDelayPrediction_Complete.py        # Complete pipeline script
├── generate_fast_visualizations.py          # Task 5 visualization generation
├── generate_explainability.py               # SHAP explainability plots
├── task1_evidence.py                        # Task 1 evidence output scripts
├── task2_ingestion.py                       # Task 2 data ingestion output
├── README.md                                # This file
└── .gitignore                               # Git ignore rules
```

---

## Task Summaries

### Task 1 — Data Understanding (Week 1–2)
- Loaded the BTS On-Time Performance CSV using PySpark
- Performed schema inspection (`printSchema`) across 110 attributes
- Generated descriptive statistics and initial `df.show(20)` preview
- Identified key features, missing values, and class imbalance in `ArrDel15`

**Key Outputs:**
- Figure 1: `df.show(20)` — First 20 records
- Figure 2: `df.printSchema()` — 110 columns with data types
- Figure 3: File size verification (235.4 MB)

---

### Task 2 — Data Engineering (Week 2)
- Cleaned and filtered data (removed cancelled/diverted flights)
- Handled missing values using median imputation
- Removed data leakage variables (`ArrDelay`, `ArrTime`, etc.)
- Applied stratified sampling (15% of each class) to manage compute cost
- Split dataset into 80% training / 20% test sets

**Key Outputs:**
- Figure 4: PySpark data ingestion with partition count (8 partitions)

---

### Task 3 — Model Development (Week 3)
- Built a reusable PySpark ML Pipeline
- Applied `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler`
- Trained 4 models:
  - Logistic Regression
  - Decision Tree Classifier
  - Random Forest Classifier
  - GBT Classifier (Gradient Boosted Trees)
- Performed 3-fold cross-validation with `ParamGridBuilder`

---

### Task 4 — Distributed Computing (Week 4)
- Analysed Spark's distributed execution strategy
- Monitored partition distribution, task scheduling, and executor logs
- Evaluated speedup and scalability of PySpark vs single-node Python

---

### Task 5 — Evaluation and Stability (Week 5)
- Generated comprehensive model evaluation metrics:
  - Accuracy, Precision, Recall, F1-Score, AUC-ROC
- Produced all required visualisations:
  - Confusion Matrices (all 4 models)
  - ROC Curves (combined chart with AUC values)
  - Precision-Recall Curves (combined chart)
  - Feature Importance (Top 15 features — GBTClassifier)
  - SHAP Summary Plot (model explainability)
- Conducted stability analysis via 5% dataset perturbation

---

### Task 6 — Tableau Export (Week 6)
- Exported model predictions and evaluation metrics for Tableau visualisation
- Generated CSV/Parquet outputs for dashboard creation
- Created interactive Tableau dashboards for stakeholder reporting

---

## Setup and Installation

### Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.12+ |
| Java (JDK) | 11 or 17 |
| Apache Spark | 3.5+ |
| PySpark | 3.5+ |
| Hadoop WinUtils | 3.x (Windows only) |

### 1. Clone the Repository

```bash
git clone https://github.com/mallikarjunnarsapuram8-sys/Mallikarjunna-.git
cd Mallikarjunna-
```

### 2. Install Python Dependencies

```bash
pip install pyspark pandas numpy matplotlib seaborn scikit-learn
```

### 3. Set Java Environment (Windows)

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH
```

### 4. Download the Dataset

Download the BTS On-Time Performance Data (January 2024) from:
[https://www.bts.gov/topics/airlines-and-airports/airline-on-time-statistics-and-delay-causes](https://www.bts.gov/topics/airlines-and-airports/airline-on-time-statistics-and-delay-causes)

Place the CSV file in the `data/` directory.

### 5. Run the Notebooks

Open any notebook in Jupyter or VS Code:
```bash
jupyter notebook notebooks/Task1.ipynb
```

Or run all visualisations directly (no Spark required):
```bash
# Generate all Task 5 evaluation plots
python generate_fast_visualizations.py

# Generate SHAP explainability plot
python generate_explainability.py

# Generate Task 1 evidence outputs
python task1_evidence.py

# Generate Task 2 ingestion output
python task2_ingestion.py
```

---

## Models and Performance

| Model | Accuracy | Precision | Recall | F1-Score | AUC-ROC |
|---|---|---|---|---|---|
| Logistic Regression | ~0.83 | ~0.82 | ~0.83 | ~0.82 | 0.8123 |
| Decision Tree | ~0.81 | ~0.80 | ~0.81 | ~0.80 | 0.7780 |
| Random Forest | ~0.86 | ~0.85 | ~0.86 | ~0.85 | 0.8540 |
| **GBTClassifier** | **~0.88** | **~0.87** | **~0.88** | **~0.87** | **0.8810** |

> **Best Model:** GBTClassifier (Gradient Boosted Trees) — highest AUC-ROC of 0.8810

---

## Key Features Identified

The GBTClassifier identified these as the most important predictors of flight delays:

1. `DepDelay` — Departure delay (strongest predictor)
2. `TaxiOut` — Time on runway before takeoff
3. `CRSDepTime` — Scheduled departure time
4. `Distance` — Flight distance
5. `AirTime` — Time in air
6. `Month` — Seasonal patterns
7. `DayOfWeek` — Day-of-week patterns
8. `Origin` — Departure airport
9. `Dest` — Arrival airport
10. `Reporting_Airline` — Airline carrier

---

## Version Control Log

| Commit | Task | Description |
|---|---|---|
| `e05ac16` | Setup | Initial commit with Task 1 notebook and related files |
| `b6d844d` | Cleanup | Removed test/debug scripts to reduce repo weight |
| `5629882` | Task 5 | Implemented evaluation notebook and automated visualisation scripts |
| `7e7e545` | Evidence | Added dataset verification, ingestion simulation, and SHAP explainability |

---

## Academic Context

- **Module:** 7006SCN – Machine Learning and Big Data
- **Programme:** MSc Big Data and Machine Learning
- **Institution:** Coventry University
- **Assessment:** Coursework Portfolio (Tasks 1–6)
- **Dataset Source:** US Bureau of Transportation Statistics (BTS)

---

## License

This project is submitted as part of assessed coursework for **7006SCN**. All code is original unless otherwise cited in the notebooks.
