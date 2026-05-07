# Step-by-Step Commands — BDA Assignment 03

Follow these commands **in order** on your WSL2/Ubuntu terminal.

---

## 1. Prerequisites Check

```bash
# Check Java is installed
java -version

# Check Hadoop is installed
hadoop version

# Check Spark is installed
spark-submit --version

# Check Python
python3 --version
```

---

## 2. Start Hadoop Services

```bash
# Start HDFS (NameNode, DataNode, SecondaryNameNode)
start-dfs.sh

# Start YARN (ResourceManager, NodeManager)
start-yarn.sh

# Verify all services are running
jps
```

**Expected JPS output:**
```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

---

## 3. Install Python Dependencies

```bash
cd ~/Desktop/BDA_A2       # or wherever your project is
pip install -r requirements.txt
```

---

## 4. Run Data Ingestion (A2 — already done)

If your data is already on HDFS from A2, **skip this step**. Otherwise:

```bash
python3 ingest.py
```

**Verify the raw data is on HDFS:**
```bash
hdfs dfs -ls /warehouse/raw/nyc_taxi/year=2026/month=04/
```

You should see `yellow_tripdata_2015-01.csv` listed.

---

## 5. Run ETL Pipeline (Task 1)

```bash
spark-submit --master local[*] etl.py
```

**What this does:**
- Reads raw CSV from HDFS
- Cleans data (duplicates, fares, passengers, distances, datetimes)
- Derives new columns (trip_duration, revenue_per_mile, time_of_day, etc.)
- Creates star schema (1 fact table + 5 dimension tables)
- Writes everything as Parquet to `/warehouse/processed/`
- Validates all tables (row counts, null checks)

**Expected output at the end:**
```
POST-LOAD VALIDATION REPORT
======================================================================
  Table: fact_trips
    Path:        /warehouse/processed/fact_trips
    Rows:        ~11,000,000+
    ...
  RAW COUNT:      ~12,700,000
  CLEANED COUNT:  ~11,000,000+
  REMOVED:        ~X,XXX,XXX (X.XX%)
======================================================================
```

**Verify Parquet files on HDFS:**
```bash
hdfs dfs -ls -R /warehouse/processed/
```

You should see directories like:
```
/warehouse/processed/fact_trips/pickup_date=2015-01-01/
/warehouse/processed/fact_trips/pickup_date=2015-01-02/
...
/warehouse/processed/dim_datetime/
/warehouse/processed/dim_vendor/
/warehouse/processed/dim_ratecode/
/warehouse/processed/dim_payment/
/warehouse/processed/dim_location/
```

**⚠️ TAKE A SCREENSHOT OF THIS OUTPUT** → save as `hdfs_screenshot.png`

---

## 6. Run Analytics & Visualizations (Task 2)

```bash
spark-submit --master local[*] analytics.py
```

**What this does:**
- Loads Parquet warehouse from HDFS
- Runs 5 Spark SQL queries with business interpretations
- Generates 4 charts saved to `visualizations/` folder

**After it finishes, check the visualizations:**
```bash
ls -la visualizations/
```

You should see:
```
chart1_daily_revenue_trend.png
chart2_revenue_by_payment.png
chart3_fare_heatmap.png
chart4_summary_dashboard.png
```

---

## 7. Copy Query Output for Report

The terminal will print all 5 query results with tables and interpretations. 
**Copy-paste this output** into your `final_report.pdf`.

---

## 8. HDFS Screenshot

```bash
hdfs dfs -ls -R /warehouse/processed/ | head -50
```

Take a screenshot and save as `hdfs_screenshot.png`.

---

## 9. Create the Submission ZIP

```bash
# Make sure you're in the project directory
cd ~/Desktop/BDA_A2

# Create the zip (replace GroupNumber with your actual group number)
zip -r GroupNumber_A3_BDA.zip \
    etl.py \
    analytics.py \
    final_report.pdf \
    hdfs_screenshot.png \
    requirements.txt \
    README.md \
    visualizations/
```

---

## Troubleshooting

### "HDFS not available" or connection error
```bash
# Check if HDFS is running
hdfs dfsadmin -report

# If not, restart
stop-dfs.sh
start-dfs.sh
```

### "Java not found" by Spark
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### Spark runs out of memory
```bash
# Give Spark more memory
spark-submit --master local[*] --driver-memory 4g --executor-memory 4g etl.py
```

### "Permission denied" on HDFS
```bash
hdfs dfs -chmod -R 777 /warehouse/
```

### Matplotlib fails (no display)
The scripts use `matplotlib.use("Agg")` for headless mode. If you get display errors, make sure the `Agg` backend is set at the top of `analytics.py`.

---

## Summary of All Commands (Quick Reference)

```bash
# 1. Start services
start-dfs.sh && start-yarn.sh && jps

# 2. Install deps
pip install -r requirements.txt

# 3. Ingest (if not done)
python3 ingest.py

# 4. Run ETL
spark-submit --master local[*] etl.py

# 5. Run Analytics
spark-submit --master local[*] analytics.py

# 6. Verify HDFS
hdfs dfs -ls -R /warehouse/processed/

# 7. Zip submission
zip -r GroupNumber_A3_BDA.zip etl.py analytics.py final_report.pdf hdfs_screenshot.png requirements.txt README.md visualizations/
```
