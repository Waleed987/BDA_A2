# 🎓 BDA Assignment 02 & 03 — Live Demo Guide

## Pre-Demo Checklist (Before Ma'am Arrives)

Open **two terminals** ready:
- **Terminal 1 (WSL):** `cd /mnt/c/Users/pc/Desktop/BDA_A2`
- **Terminal 2 (WSL):** Same directory (for monitoring)

Make sure Hadoop services are running:
```bash
start-dfs.sh
start-yarn.sh
jps
```
✅ You should see: `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`

If any service is missing, run:
```bash
stop-all.sh
start-dfs.sh
start-yarn.sh
```

---

## Part A — Assignment 02 Demo (Data Ingestion & Profiling)

### Step 1: Show the Raw Dataset
> **Say:** "Ma'am, our dataset is the NYC Yellow Taxi Trip Data from January 2015, approximately 1.9 GB with ~12.7 million rows and 19 columns."

```bash
ls -lh yellow_tripdata_2015-01.csv
```
> This shows the file size (~1.9 GB).

```bash
head -5 yellow_tripdata_2015-01.csv
```
> This shows the first few rows and column headers.

---

### Step 2: Show the Ingestion Script
> **Say:** "Our ingestion script validates the file (encoding, integrity, row count), then uploads it to HDFS in a partitioned directory structure."

```bash
cat ingest.py
```
> Quickly scroll through and point out:
> - File validation (encoding detection, row count)
> - HDFS directory creation (`/warehouse/raw/nyc_taxi/year=2026/month=04/`)
> - Upload via `hdfs dfs -put`
> - Logging

---

### Step 3: Show Data Already on HDFS
> **Say:** "The data has already been ingested to HDFS. Let me show you the directory structure."

```bash
hdfs dfs -ls -R /warehouse/raw/
```

**Expected Output:**
```
drwxr-xr-x   - waleed supergroup   0 /warehouse/raw/nyc_taxi
drwxr-xr-x   - waleed supergroup   0 /warehouse/raw/nyc_taxi/year=2026
drwxr-xr-x   - waleed supergroup   0 /warehouse/raw/nyc_taxi/year=2026/month=04
-rw-r--r--   1 waleed supergroup   1985964692 yellow_tripdata_2015-01.csv
```

> **Say:** "As you can see, the raw CSV (~1.9 GB) is stored in HDFS with year/month partitioning."

---

### Step 4: Show the Profiling Script
> **Say:** "We then profiled the data to identify quality issues."

```bash
cat profile_data.py
```
> Point out:
> - Null/missing value detection
> - Statistical summaries
> - Distribution analysis
> - Data quality findings

---

### Step 5: Show Profiling Output & Cleaning Strategy
> **Say:** "Based on profiling, we documented a cleaning strategy."

```bash
cat cleaning_strategy.md
```
> Highlight key findings:
> - Zero passenger counts
> - Negative fares
> - Extreme trip distances
> - Invalid datetime values
> - Missing values in multiple columns

---

## Part B — Assignment 03 Demo (ETL Pipeline & Analytics)

### Step 6: Show the ETL Script Structure
> **Say:** "Now for Assignment 3. Our ETL pipeline picks up from where A2 left off. Let me walk you through the code first."

```bash
head -90 etl.py
```

> **Key points to mention:**
> 1. "We read the raw CSV from HDFS"
> 2. "Every transformation has an inline comment referencing the A2 profiling finding"
> 3. "We use an explicit schema for type safety"

---

### Step 7: Show Transformations (Scroll Through)
> **Say:** "Each transformation is traceable back to our A2 findings."

```bash
grep -n "A2 Finding" etl.py
```

> This shows all A2 traceability comments at a glance. Point out:
> - `2a. Remove duplicates` → A2 Finding: duplicates detected
> - `2b. Convert datetime` → A2 Finding: loaded as string
> - `2c. Fix zero passenger count` → A2 Finding: 0 values
> - `2d. Fix negative fares` → A2 Finding: fare ≤ 0
> - `2e. Cap extreme distances` → A2 Finding: > 100 miles
> - etc.

---

### Step 8: Show Derived Columns
> **Say:** "We derived several new columns for analysis."

```bash
grep -n "withColumn" etl.py | head -20
```

> Mention key derived columns:
> - `trip_duration_min` — duration in minutes
> - `revenue_per_mile` — fare / distance
> - `avg_speed_mph` — speed calculation
> - `time_of_day` — Morning/Afternoon/Evening/Night
> - `tip_percentage` — tip as % of fare
> - `distance_bucket` — Short/Medium/Long categorization
> - `payment_label` / `ratecode_label` — normalized categoricals

---

### Step 9: Run the ETL Pipeline (LIVE)
> **Say:** "Let me run the ETL pipeline now."

```bash
spark-submit --master local[*] --driver-memory 4g etl.py
```

> ⏱️ **This takes ~8-10 minutes.** While it runs, explain:
> - "We write fact_trips first as Parquet, partitioned by pickup_date"
> - "Then we derive 5 dimension tables from the Parquet output"
> - "Finally, we run validation — row counts and null checks"

> **If ma'am doesn't want to wait**, you can skip to Step 10 (show pre-existing output).

---

### Step 10: Show HDFS Output (After ETL or Pre-existing)
> **Say:** "Let me show you the processed warehouse structure on HDFS."

```bash
hdfs dfs -ls -R /warehouse/processed/ | head -40
```

> **Point out:**
> - `fact_trips/` — partitioned by `pickup_date=YYYY-MM-DD`
> - `dim_datetime/`, `dim_vendor/`, `dim_ratecode/`, `dim_payment/`, `dim_location/`
> - Files are in `.parquet` format (compressed, columnar)

To show partition structure specifically:
```bash
hdfs dfs -ls /warehouse/processed/fact_trips/ | head -10
```

> **Say:** "Each date has its own partition folder — this enables partition pruning for date-range queries."

---

### Step 11: Show the Validation Report
> **Say:** "The ETL output includes a validation report with row counts and null checks."

> If you saved the ETL output, show it. Otherwise mention:
> - Raw count: ~12.7 million
> - Cleaned count: ~12.0 million (approximately 5-6% removed)
> - All dimension tables have zero nulls ✓
> - fact_trips shows nulls only in optional columns (lat/long)

---

### Step 12: Show Analytics Script Structure
> **Say:** "Now let's look at the analytics — 5 Spark SQL queries answering business questions."

```bash
head -60 analytics.py
```

> Point out:
> - Reads Parquet warehouse from HDFS
> - Caches fact table for reuse across queries
> - Registers as Spark SQL temp views

---

### Step 13: Show the 5 Queries
> **Say:** "Let me show you our 5 business questions."

```bash
grep -n "QUERY\|Business Q\|WINDOW\|RANK\|ROW_NUMBER\|LAG\|BROADCAST" analytics.py
```

> Walk through each query:
>
> | Query | Business Question | Special Feature |
> |-------|-------------------|-----------------|
> | Q1 | Peak revenue hours | Time-based analysis (hourly) |
> | Q2 | Top revenue days | **RANK()** window function |
> | Q3 | Payment type & tips | **ROW_NUMBER()** window function |
> | Q4 | Rate code analysis | **Broadcast join** with dim table |
> | Q5 | Daily revenue trend | **LAG()** window function |

---

### Step 14: Run Analytics (LIVE)
> **Say:** "Let me run the analytics now."

```bash
spark-submit --master local[*] --driver-memory 4g analytics.py
```

> ⏱️ **This takes ~2-3 minutes.** While it runs, explain:
> - "Each query output includes a business interpretation"
> - "We generate 4 different chart types"
> - "We also show a query execution plan for optimization"

---

### Step 15: Show Visualizations
> **Say:** "Here are the 4 visualizations generated from our warehouse data."

Open the charts (from Windows File Explorer or command line):
```bash
# From Windows PowerShell or cmd:
start visualizations\chart1_daily_revenue_trend.png
start visualizations\chart2_revenue_by_payment.png
start visualizations\chart3_fare_heatmap.png
start visualizations\chart4_summary_dashboard.png
```

Or from WSL:
```bash
explorer.exe visualizations/
```

> Walk through each chart:
>
> | Chart | Type | Key Insight |
> |-------|------|-------------|
> | Chart 1 | Line/Area | Weekly revenue cycle, blizzard dip on Jan 26-27 |
> | Chart 2 | Bar | Credit card dominates revenue (~65%) |
> | Chart 3 | Heatmap | Highest fares at 4-6 AM (airport trips) |
> | Chart 4 | Dashboard | 4 subplots — holistic trip analysis |

---

### Step 16: Show Optimizations (Task 3)
> **Say:** "We applied 4 optimization techniques."

**1. Partitioning:**
```bash
hdfs dfs -ls /warehouse/processed/fact_trips/ | head -5
```
> "fact_trips is partitioned by pickup_date — this enables partition pruning."

**2. Caching/Persist:**
```bash
grep -n "persist\|cache" etl.py analytics.py
```
> "We persist DataFrames with MEMORY_AND_DISK for reuse."

**3. Broadcast Join:**
```bash
grep -n "broadcast" analytics.py
```
> "Small dimension tables are broadcast to avoid expensive shuffles."

**4. Query Plan Analysis:**
> "The .explain(True) output shows predicate pushdown and partition pruning in action."
> Show the query plan from the ETL or analytics output.

---

## Quick Recovery Commands (If Something Goes Wrong)

### If Hadoop services are down:
```bash
stop-all.sh
start-dfs.sh
start-yarn.sh
jps
```

### If HDFS is in safe mode:
```bash
hdfs dfsadmin -safemode leave
```

### If etl.py gets killed (OOM):
```bash
# Use more memory
spark-submit --master local[*] --driver-memory 8g etl.py
```

### If you need to clear processed data and re-run:
```bash
hdfs dfs -rm -r /warehouse/processed/
spark-submit --master local[*] --driver-memory 4g etl.py
```

### Check if data exists:
```bash
hdfs dfs -ls -R /warehouse/raw/
hdfs dfs -ls -R /warehouse/processed/
```

---

## Summary Table (What Fulfills What)

| Rubric Requirement | Where to Show | Marks |
|--------------------|---------------|-------|
| ETL Transform | `etl.py` lines 85-255 (grep "A2 Finding") | 10 |
| ETL Load (Parquet) | `hdfs dfs -ls -R /warehouse/processed/` | — |
| ETL Validate | Validation report in ETL output | — |
| A2 Traceability | `grep "A2 Finding" etl.py` | — |
| 5 Spark SQL queries | `analytics.py` Q1-Q5 output | 8 |
| 2 Window functions | Q2 (RANK), Q3 (ROW_NUMBER), Q5 (LAG) | — |
| Time-based analysis | Q1 (hourly), Q5 (daily trend) | — |
| Business interpretations | Printed after each query | — |
| 4 Visualizations | `visualizations/` folder | 6 |
| Partitioning | `fact_trips/pickup_date=*` on HDFS | 3 |
| Caching | `persist(MEMORY_AND_DISK)` in code | — |
| Broadcast join | Q4 in analytics.py | — |
| Query plan | `.explain(True)` output | — |
| Final report | `final_report.pdf` | 3 |

**Total: 30 marks**

---

## 🎯 Pro Tips for the Demo

1. **Keep the terminal font large** — so ma'am can read from a distance
2. **Have the charts already open** — don't waste time waiting for file explorer
3. **Know your numbers** — "12.7M raw rows, ~12.0M after cleaning, ~5-6% removed"
4. **Mention A2 traceability** — "Every transformation references our A2 profiling finding"
5. **If ETL takes too long** — show the pre-existing HDFS output and run analytics only
6. **Be confident about optimizations** — know what partitioning, caching, broadcast, and query plans do
