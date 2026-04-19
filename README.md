# BDA Assignment 02 — HDFS Data Ingestion & Profiling

## Project Overview

This project implements a **fully automated data ingestion pipeline** for the NYC Yellow Taxi Trip dataset (January 2015) into the Hadoop Distributed File System (HDFS). The pipeline includes file validation, encoding detection, row count verification, and structured HDFS upload with partitioned directory organization. A comprehensive data profiling report is also generated to analyze schema, missing values, statistical summaries, and distributions.

### Dataset

- **Source:** [NYC Yellow Taxi Trip Data — Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **File:** `yellow_tripdata_2015-01.csv`
- **Size:** ~1.9 GB
- **Records:** 2,000,000 rows × 19 columns

---

## Repository Structure

```
BDA_A2/
├── ingest.py                  # Fully automated HDFS ingestion script
├── profile_data.py            # Data profiling script (schema, stats, visualizations)
├── cleaning_strategy.md       # Proposed cleaning strategy for Assignment 3
├── requirements.txt           # Python dependencies
├── hdfs_screenshot.png        # Screenshot of HDFS directory structure
├── profiling_report.pdf       # Data profiling report
├── ingestion.log              # Auto-generated log from ingest.py
├── missing_heatmap.png        # Missing values heatmap
├── dist_*.png                 # Distribution plots for numeric columns
└── README.md                  # This file
```

---

## Setup Instructions

### Prerequisites

- **OS:** Ubuntu (WSL2 on Windows or native Linux)
- **Python:** 3.8+
- **Hadoop:** 3.x (HDFS configured and running)
- **Java:** JDK 8 or 11

### 1. Clone the Repository

```bash
git clone https://github.com/Waleed987/BDA_A2.git
cd BDA_A2
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

This installs:
| Package | Version | Purpose |
|---------|---------|---------|
| pandas | 2.2.0 | Data loading and profiling |
| matplotlib | 3.8.2 | Distribution visualizations |
| seaborn | 0.13.2 | Heatmap and statistical plots |
| chardet | 5.2.0 | File encoding detection |

### 3. Start Hadoop Services

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

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) and place the CSV file in the **same directory** as the scripts:

```
BDA_A2/
├── ingest.py
├── profile_data.py
└── yellow_tripdata_2015-01.csv   ← place here
```

> **Note:** Both scripts automatically detect their own directory — no hardcoded paths to edit.

---

## How to Run `ingest.py`

The ingestion script is **fully automated** — no user input is required.

```bash
python3 ingest.py
```

### What It Does (Step-by-Step)

1. **File Validation**
   - Checks file existence and `.csv` extension
   - Validates file size (warns if < 1 MB)
   - Detects file encoding using `chardet`
   - Counts total rows using `wc -l`

2. **HDFS Upload**
   - Creates the target HDFS directory: `/warehouse/raw/nyc_taxi/year=2026/month=04`
   - Uploads the CSV file using `hdfs dfs -put -f`

3. **Logging**
   - All steps are logged to both the console and `~/ingestion.log`
   - Errors and warnings are captured with timestamps

### Verify Upload

After running the script, verify the file is on HDFS:

```bash
hdfs dfs -ls -R /warehouse/raw/nyc_taxi/
```

---

## Data Profiling

Run the profiling script to generate schema info, missing value analysis, statistical summaries, and distribution plots:

```bash
python3 profile_data.py
```

This generates:
- Console output with schema, missing values, and descriptive statistics
- `missing_heatmap.png` — heatmap of null values across all columns
- `dist_*.png` — distribution histograms for key numeric columns

---

## Group Members

| Name | Contribution |
|------|--------------|
| Muhammad Waleed Younas | Ingestion pipeline, HDFS setup, data profiling |
| Syed Danish Abbas | Data profiling, cleaning strategy |
| Daniyal | Data profiling, documentation |

---

## Submission Details

- **Course:** Big Data Analytics
- **Assignment:** 02 — Data Ingestion & Profiling
- **Submission Date:** April 2026
- **Submitted Files:**

| File | Description |
|------|-------------|
| `ingest.py` | Fully automated Python ingestion script |
| `hdfs_screenshot.png` | Screenshot of HDFS showing uploaded files and directory structure |
| `profiling_report.pdf` | Data profiling report with schema, stats, and visualizations |
| `requirements.txt` | All Python dependencies listed |
| `README.md` | Project documentation (this file) |

---
