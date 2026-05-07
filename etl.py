"""
etl.py — PySpark ETL Pipeline (Assignment 03, Task 1)
=====================================================
Picks up from A2 ingested data on HDFS, applies cleaning & transformation,
models into a star schema (fact + dimension tables), writes Parquet to
/warehouse/processed/, and validates the output.

Dataset: NYC Yellow Taxi Trip Data — January 2015
HDFS Source: /warehouse/raw/nyc_taxi/year=2026/month=04/yellow_tripdata_2015-01.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)
from pyspark import StorageLevel
import logging
import sys

# ──────────────────────────────────────────────────────
# Logging setup
# ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("ETL")

# ──────────────────────────────────────────────────────
# Spark session
# ──────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("NYC_Taxi_ETL_Pipeline")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

log.info("Spark session created successfully.")

# ──────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────
# Use the exact path from your HDFS ls output
HDFS_RAW = "/warehouse/raw/nyc_taxi/year=2026/month=04/yellow_tripdata_2015-01.csv"
HDFS_PROCESSED = "/warehouse/processed"

# ──────────────────────────────────────────────────────
# STEP 1 — Read raw CSV from HDFS
# ──────────────────────────────────────────────────────
log.info("Reading raw CSV from HDFS: %s", HDFS_RAW)

schema = StructType([
    StructField("VendorID",              IntegerType(),  True),
    StructField("tpep_pickup_datetime",  StringType(),   True),
    StructField("tpep_dropoff_datetime", StringType(),   True),
    StructField("passenger_count",       IntegerType(),  True),
    StructField("trip_distance",         DoubleType(),   True),
    StructField("pickup_longitude",      DoubleType(),   True),
    StructField("pickup_latitude",       DoubleType(),   True),
    StructField("RatecodeID",            IntegerType(),  True),
    StructField("store_and_fwd_flag",    StringType(),   True),
    StructField("dropoff_longitude",     DoubleType(),   True),
    StructField("dropoff_latitude",      DoubleType(),   True),
    StructField("payment_type",          IntegerType(),  True),
    StructField("fare_amount",           DoubleType(),   True),
    StructField("extra",                 DoubleType(),   True),
    StructField("mta_tax",              DoubleType(),   True),
    StructField("tip_amount",            DoubleType(),   True),
    StructField("tolls_amount",          DoubleType(),   True),
    StructField("improvement_surcharge", DoubleType(),   True),
    StructField("total_amount",          DoubleType(),   True),
])

raw_df = spark.read.csv(HDFS_RAW, header=True, schema=schema)
log.info("Raw data loaded (schema applied, count deferred).")

# ──────────────────────────────────────────────────────
# STEP 2 — Cleaning & Transformation
#   Every transformation references the A2 profiling finding.
# ──────────────────────────────────────────────────────
log.info("Starting transformations...")

# --- 2a. Remove duplicates ---------------------------------------------------
# A2 Finding: Duplicate rows detected (profile_data.py line 47-49).
# Action: Drop fully duplicated rows, keep first occurrence.
df = raw_df.dropDuplicates()
log.info("Dedup transform applied (lazy).")

# --- 2b. Convert datetime strings to TimestampType ---------------------------
# A2 Finding: tpep_pickup_datetime and tpep_dropoff_datetime loaded as string
#   (object) instead of datetime, affecting 100 % of rows (profile_data.py line 67).
# Action: Cast to timestamp; rows with unparseable values become null and are dropped.
df = df.withColumn("tpep_pickup_datetime",
                   F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("tpep_dropoff_datetime",
                   F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

# Drop rows where datetime parsing failed (NaT equivalent)
df = df.filter(F.col("tpep_pickup_datetime").isNotNull() &
               F.col("tpep_dropoff_datetime").isNotNull())

# --- 2c. Fix zero passenger count --------------------------------------------
# A2 Finding: passenger_count contains 0 values, physically impossible
#   (profile_data.py line 57-59).
# Action: Replace 0 with mode (1), since ~70 % of trips are single-passenger.
df = df.withColumn("passenger_count",
                   F.when(F.col("passenger_count") == 0, 1)
                    .otherwise(F.col("passenger_count")))

# --- 2d. Fix negative / zero fare amounts ------------------------------------
# A2 Finding: fare_amount contains rows ≤ 0, logically invalid
#   (profile_data.py line 52-54).
# Action: Drop rows with fare_amount <= 0 (cancelled / test trips).
df = df.filter(F.col("fare_amount") > 0)

# --- 2e. Cap extreme trip distances ------------------------------------------
# A2 Finding: trip_distance > 100 miles is unrealistic for NYC taxis
#   (profile_data.py line 62-64).
# Action: Cap at 100 miles (winsorize).
df = df.withColumn("trip_distance",
                   F.when(F.col("trip_distance") > 100, 100.0)
                    .otherwise(F.col("trip_distance")))

# Also drop zero / negative distances
df = df.filter(F.col("trip_distance") > 0)

# --- 2f. Fix invalid store_and_fwd_flag --------------------------------------
# A2 Finding: store_and_fwd_flag should only be 'Y' or 'N'
#   (profile_data.py line 70-72).
# Action: Replace any invalid value with 'N' (dominant category).
df = df.withColumn("store_and_fwd_flag",
                   F.when(F.col("store_and_fwd_flag").isin("Y", "N"),
                          F.col("store_and_fwd_flag"))
                    .otherwise("N"))

# --- 2g. Fix negative tip / tolls / surcharge --------------------------------
# A2 Finding: tip_amount, tolls_amount, improvement_surcharge, extra may
#   contain negative values (cleaning_strategy.md §8).
# Action: Replace negatives with 0.
for col_name in ["tip_amount", "tolls_amount", "improvement_surcharge", "extra", "mta_tax"]:
    df = df.withColumn(col_name,
                       F.when(F.col(col_name) < 0, 0.0)
                        .otherwise(F.col(col_name)))

# --- 2h. Impute nulls in low-impact columns ----------------------------------
# A2 Finding: Missing values detected across multiple columns
#   (profile_data.py line 27).
# Action: Impute with mode/default values per cleaning_strategy.md §7.
df = df.fillna({
    "passenger_count": 1,       # mode
    "RatecodeID":      1,       # Standard rate (mode)
    "payment_type":    1,       # Credit card (mode)
    "store_and_fwd_flag": "N",  # dominant category
})
# Drop rows with null in critical columns (fare, distance, total)
df = df.filter(F.col("fare_amount").isNotNull() &
               F.col("trip_distance").isNotNull() &
               F.col("total_amount").isNotNull())

# --- 2i. Derive new columns --------------------------------------------------

# Recalculate total_amount to ensure consistency
# A2 Finding: total_amount may not match sum of components after cleaning.
df = df.withColumn("total_amount_recalc",
    F.col("fare_amount") + F.col("extra") + F.col("mta_tax") +
    F.col("tip_amount") + F.col("tolls_amount") + F.col("improvement_surcharge")
)

# Trip duration in minutes
df = df.withColumn("trip_duration_min",
    F.round(
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2
    ))
# Remove impossible durations (<=0 or > 300 min / 5 hours)
df = df.filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 300))

# Revenue per mile
df = df.withColumn("revenue_per_mile",
    F.round(F.col("fare_amount") / F.col("trip_distance"), 2))

# Speed in mph
df = df.withColumn("avg_speed_mph",
    F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2))
# Cap speed at 80 mph (unrealistic above that for NYC)
df = df.withColumn("avg_speed_mph",
    F.when(F.col("avg_speed_mph") > 80, 80.0).otherwise(F.col("avg_speed_mph")))

# Time-of-day flag (Morning / Afternoon / Evening / Night)
df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
df = df.withColumn("time_of_day",
    F.when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") < 12), "Morning")
     .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 17), "Afternoon")
     .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") < 21), "Evening")
     .otherwise("Night")
)

# Day of week
df = df.withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
df = df.withColumn("day_name",
    F.when(F.col("pickup_day_of_week") == 1, "Sunday")
     .when(F.col("pickup_day_of_week") == 2, "Monday")
     .when(F.col("pickup_day_of_week") == 3, "Tuesday")
     .when(F.col("pickup_day_of_week") == 4, "Wednesday")
     .when(F.col("pickup_day_of_week") == 5, "Thursday")
     .when(F.col("pickup_day_of_week") == 6, "Friday")
     .otherwise("Saturday")
)

# Is weekend flag
df = df.withColumn("is_weekend",
    F.when(F.col("pickup_day_of_week").isin(1, 7), True).otherwise(False))

# Pickup date (for partitioning)
df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
df = df.withColumn("pickup_month", F.month("tpep_pickup_datetime"))
df = df.withColumn("pickup_year", F.year("tpep_pickup_datetime"))

# Tip percentage
df = df.withColumn("tip_percentage",
    F.round(F.when(F.col("fare_amount") > 0,
                   (F.col("tip_amount") / F.col("fare_amount")) * 100)
             .otherwise(0.0), 2))

# Distance bucket (short / medium / long)
df = df.withColumn("distance_bucket",
    F.when(F.col("trip_distance") <= 2, "Short (0-2 mi)")
     .when(F.col("trip_distance") <= 10, "Medium (2-10 mi)")
     .otherwise("Long (10+ mi)")
)

# Payment type label (normalize categorical)
df = df.withColumn("payment_label",
    F.when(F.col("payment_type") == 1, "Credit Card")
     .when(F.col("payment_type") == 2, "Cash")
     .when(F.col("payment_type") == 3, "No Charge")
     .when(F.col("payment_type") == 4, "Dispute")
     .otherwise("Unknown")
)

# Rate code label (normalize categorical)
df = df.withColumn("ratecode_label",
    F.when(F.col("RatecodeID") == 1, "Standard")
     .when(F.col("RatecodeID") == 2, "JFK")
     .when(F.col("RatecodeID") == 3, "Newark")
     .when(F.col("RatecodeID") == 4, "Nassau/Westchester")
     .when(F.col("RatecodeID") == 5, "Negotiated")
     .when(F.col("RatecodeID") == 6, "Group Ride")
     .otherwise("Unknown")
)

# ──────────────────────────────────────────────────────
# STEP 3 — Write Fact Table FIRST, then derive dimensions from Parquet
#   Strategy: Write fact_trips from the lazy plan (single pass through CSV),
#   then read back the compressed Parquet to derive small dimension tables.
#   This avoids keeping the entire raw DataFrame in memory.
# ──────────────────────────────────────────────────────
log.info("Writing fact_trips to HDFS as Parquet (partitioned by pickup_date)...")

# --- Write fact_trips from lazy plan (single pass through raw CSV) --------
fact_trips = df.select(
    F.monotonically_increasing_id().alias("trip_id"),
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
    "RatecodeID", "store_and_fwd_flag", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge",
    "total_amount", "total_amount_recalc",
    "trip_duration_min", "revenue_per_mile", "avg_speed_mph",
    "pickup_hour", "time_of_day", "day_name", "is_weekend",
    "pickup_date", "pickup_month", "pickup_year",
    "tip_percentage", "distance_bucket", "payment_label", "ratecode_label",
)

fact_path = f"{HDFS_PROCESSED}/fact_trips"
# OPTIMIZATION: Partitioning — fact table partitioned by pickup_date
fact_trips.write.mode("overwrite").partitionBy("pickup_date").parquet(fact_path)
log.info("  ✓ fact_trips written to %s", fact_path)

# ──────────────────────────────────────────────────────
# STEP 4 — Derive & Write Dimension Tables from Parquet
#   Reading from compressed Parquet is far more memory-efficient
#   than re-processing the raw 1.9 GB CSV.
#   OPTIMIZATION: Caching — persist Parquet fact table for reuse
# ──────────────────────────────────────────────────────
log.info("Deriving dimension tables from Parquet fact_trips...")
fact_pq = spark.read.parquet(fact_path)
fact_pq.persist(StorageLevel.MEMORY_AND_DISK)

# --- Dimension: dim_datetime --------------------------------------------------
dim_datetime = fact_pq.select(
    F.col("tpep_pickup_datetime").alias("datetime_key"),
    F.col("pickup_date").alias("date"),
    F.col("pickup_year").alias("year"),
    F.col("pickup_month").alias("month"),
    F.dayofmonth("tpep_pickup_datetime").alias("day"),
    F.col("pickup_hour").alias("hour"),
    F.dayofweek("tpep_pickup_datetime").alias("day_of_week"),
    F.col("day_name"),
    F.col("time_of_day"),
    F.col("is_weekend"),
).dropDuplicates(["date", "hour"])

# --- Dimension: dim_location --------------------------------------------------
dim_location = fact_pq.select(
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
).dropDuplicates()
dim_location = dim_location.withColumn("location_id", F.monotonically_increasing_id())

# --- Dimension: dim_vendor ----------------------------------------------------
dim_vendor = fact_pq.select("VendorID").dropDuplicates()
dim_vendor = dim_vendor.withColumn("vendor_name",
    F.when(F.col("VendorID") == 1, "Creative Mobile Technologies")
     .when(F.col("VendorID") == 2, "VeriFone Inc.")
     .otherwise("Unknown")
)

# --- Dimension: dim_ratecode --------------------------------------------------
dim_ratecode = fact_pq.select("RatecodeID", "ratecode_label").dropDuplicates()

# --- Dimension: dim_payment ---------------------------------------------------
dim_payment = fact_pq.select("payment_type", "payment_label").dropDuplicates()

# Write all dimension tables
log.info("Writing dimension tables to HDFS...")
for tbl_name, tbl_df in [("dim_datetime", dim_datetime), ("dim_vendor", dim_vendor),
                          ("dim_ratecode", dim_ratecode), ("dim_payment", dim_payment),
                          ("dim_location", dim_location)]:
    path = f"{HDFS_PROCESSED}/{tbl_name}"
    tbl_df.write.mode("overwrite").parquet(path)
    log.info("  ✓ %s written to %s", tbl_name, path)

# Free persisted Parquet fact table
fact_pq.unpersist()
spark.catalog.clearCache()

# ──────────────────────────────────────────────────────
# STEP 5 — Validation (performed AFTER writes to save memory)
#   Rubric: "Verify raw vs. post-ETL counts"
# ──────────────────────────────────────────────────────

log.info("Running post-load validation...")

# --- 5a. Get raw row count (lightweight: count lines, subtract header) ----
raw_count = spark.read.text(HDFS_RAW).count() - 1  # subtract CSV header
log.info("Raw row count: %d", raw_count)

# --- 5b. Validate each table from Parquet ---------------------------------
print("\n" + "=" * 70)
print("POST-LOAD VALIDATION REPORT")
print("=" * 70)

cleaned_count = 0
validation_tables = [
    ("fact_trips",   ["pickup_date"]),
    ("dim_datetime", None),
    ("dim_vendor",   None),
    ("dim_ratecode", None),
    ("dim_payment",  None),
    ("dim_location", None),
]
for table_name, partition_cols in validation_tables:
    path = f"{HDFS_PROCESSED}/{table_name}"
    loaded = spark.read.parquet(path)
    row_count = loaded.count()
    col_count = len(loaded.columns)

    if table_name == "fact_trips":
        cleaned_count = row_count

    # OPTIMIZED: Single-pass null check using aggregation (instead of N separate counts)
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in loaded.columns
    ]
    null_row = loaded.agg(*null_exprs).collect()[0]
    null_checks = {c: null_row[c] for c in loaded.columns if null_row[c] > 0}

    print(f"\n  Table: {table_name}")
    print(f"    Path:        {path}")
    print(f"    Rows:        {row_count:,}")
    print(f"    Columns:     {col_count}")
    if null_checks:
        print(f"    Null cols:   {null_checks}")
    else:
        print(f"    Null cols:   None ✓")
    if partition_cols:
        print(f"    Partitioned: {partition_cols}")

    # Free memory after each table
    loaded.unpersist()

# --- 5c. Raw vs Cleaned summary (rubric requirement) ----------------------
print("\n" + "=" * 70)
print(f"  RAW COUNT:      {raw_count:,}")
print(f"  CLEANED COUNT:  {cleaned_count:,}")
removed = raw_count - cleaned_count
pct = (removed / raw_count) * 100 if raw_count > 0 else 0
print(f"  REMOVED:        {removed:,} ({pct:.2f}%)")
print("=" * 70)

# OPTIMIZATION: Query plan analysis for fact_trips
print("\n\n=== QUERY PLAN ANALYSIS (fact_trips read) ===")
fact_loaded = spark.read.parquet(f"{HDFS_PROCESSED}/fact_trips")
fact_loaded.filter(
    (F.col("pickup_date") == "2015-01-15") & (F.col("fare_amount") > 50)
).explain(True)
print("=== END QUERY PLAN ===\n")

log.info("ETL pipeline completed successfully.")
spark.stop()

