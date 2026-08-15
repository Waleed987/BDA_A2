# Big Data Analysis — ETL Pipeline & Analytics

## Project Overview

This project extends Assignment 02 (HDFS Data Ingestion & Profiling) into a complete data warehousing lifecycle. It implements a PySpark ETL pipeline that transforms the cleaned NYC Yellow Taxi dataset into a star-schema warehouse stored as Parquet on HDFS,runs Spark SQL analytical queries to answer business questions, and generates data visualizations.

### Dataset

- **Source:** [NYC Yellow Taxi Trip Data — Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **File:** `yellow_tripdata_2015-01.csv`
- **Size:** ~1.9 GB
- **Records:** ~12.7 million rows × 19 columns

---

## Repository Structure

```
BDA_A2/
├── ingest.py                  # (A2) Fully automated HDFS ingestion script
├── profile_data.py            # (A2) Data profiling script
├── cleaning_strategy.md       # (A2) Proposed cleaning strategy
├── etl.py                     # (A3) PySpark ETL — transform, model, load, validate
├── analytics.py               # (A3) Spark SQL queries & visualization script
├── commands.md                # (A3) Step-by-step commands to run everything
├── requirements.txt           # All Python dependencies (A2 + A3)
├── visualizations/            # Output folder for all charts
│   ├── chart1_daily_revenue_trend.png
│   ├── chart2_revenue_by_payment.png
│   ├── chart3_fare_heatmap.png
│   └── chart4_summary_dashboard.png
├── final_report.pdf           # Complete report covering all 3 tasks
├── hdfs_screenshot.png        # HDFS /warehouse/processed/ directory screenshot
└── README.md                  # This file
```

---

## Setup Instructions

### Prerequisites

- **OS:** Ubuntu (WSL2 on Windows or native Linux)
- **Python:** 3.8+
- **Java:** JDK 8 or 11
- **Hadoop:** 3.x (HDFS configured and running)
- **Apache Spark:** 3.x (with PySpark)

### 1. Clone the Repository

```bash
git clone https://github.com/Waleed987/BDA_A2.git
cd BDA_A2
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

| Package    | Version | Purpose                          |
|------------|---------|----------------------------------|
| pandas     | 2.2.0   | Data loading and profiling       |
| matplotlib | 3.8.2   | Distribution visualizations      |
| seaborn    | 0.13.2  | Heatmap and statistical plots    |
| chardet    | 5.2.0   | File encoding detection          |
| pyspark    | 3.5.1   | ETL pipeline and Spark SQL       |

### 3. Start Hadoop & Spark Services

```bash
start-dfs.sh
start-yarn.sh
```

Verify all services are running:
```bash
jps
```

Expected output should include: `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`, `SecondaryNameNode`.

### 4. Place the Dataset

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) and place the CSV file in the same directory as the scripts.

---

## How to Run

### Step 1: Data Ingestion (from A2)

```bash
python3 ingest.py
```

This validates and uploads the raw CSV to HDFS at `/warehouse/raw/nyc_taxi/year=2026/month=04/`.

### Step 2: ETL Pipeline (A3 Task 1)

```bash
spark-submit --master local[*] --driver-memory 8g etl.py
```

This script:
1. **Reads** raw CSV from HDFS
2. **Cleans** data (removes duplicates, fixes fares, passengers, distances, datetimes, etc.)
3. **Transforms** — derives new columns (trip_duration, revenue_per_mile, time_of_day, etc.)
4. **Models** data into a star schema (fact_trips + 5 dimension tables)
5. **Loads** all tables as Parquet to `/warehouse/processed/`
6. **Validates** row counts and null checks for every table

### Step 3: Analytics & Visualizations (A3 Task 2)

```bash
spark-submit --master local[*] --driver-memory 8g analytics.py
```

This script:
1. Loads the Parquet warehouse from HDFS
2. Runs 5 Spark SQL queries answering business questions
3. Generates 4 charts saved to `visualizations/` folder

### Step 4: HDFS Screenshot

```bash
hdfs dfs -ls -R /warehouse/processed/
```

Take a screenshot of the output for the report.

---

## Business Questions Answered

| # | Question | Query Features |
|---|----------|----------------|
| 1 | Peak revenue hours & rush-hour vs off-peak fares | Time-based grouping |
| 2 | Top revenue days with ranking | RANK() window function |
| 3 | Payment type split & tip comparison | ROW_NUMBER() window function |
| 4 | Rate code analysis & per-trip revenue | Broadcast join with dimension |
| 5 | Daily revenue trend & day-over-day change | LAG() window function |

## Optimizations Applied

| Technique | Where | Impact |
|-----------|-------|--------|
| **Partitioning** | fact_trips partitioned by `pickup_date` | Enables partition pruning on date-range queries |
| **Caching** | Cleaned DataFrame & fact table cached | Avoids recomputation across multiple queries |
| **Broadcast Join** | dim_ratecode joined with broadcast() | Avoids shuffle for small dimension tables |
| **Query Plan Analysis** | `.explain(True)` on complex aggregation | Demonstrates predicate pushdown & partition pruning |

---

## Visualizations

| Chart | Type | File |
|-------|------|------|
| Daily Revenue Trend | Line/Area chart | `chart1_daily_revenue_trend.png` |
| Revenue by Payment Type | Bar chart | `chart2_revenue_by_payment.png` |
| Avg Fare Heatmap (Hour × Day) | Heatmap | `chart3_fare_heatmap.png` |
| Summary Dashboard | 4 subplots | `chart4_summary_dashboard.png` |



## Submission Details

- **Course:** CS-404 Big Data Analytics
- **Assignment:** 03 — ETL Pipeline & Analytics
- **Submission Date:** May 2026

| File | Description |
|------|-------------|
| `etl.py` | PySpark transformation, modelling, loading, and validation script |
| `analytics.py` | Spark SQL queries and visualization script |
| `final_report.pdf` | Complete report covering all 3 tasks |
| `hdfs_screenshot.png` | /warehouse/processed/ directory screenshot |
| `requirements.txt` | All Python dependencies |
| `README.md` | This file |
