# A2 Traceability Table
**Copy this table directly into your `final_report.pdf`**

This table maps every A2 data quality finding to the transformation applied in `etl.py` and the PySpark function used.

---

| # | A2 Data Quality Issue | Source Reference | Transformation Applied | PySpark Function | etl.py Step |
|---|----------------------|------------------|----------------------|-----------------|-------------|
| 1 | Duplicate rows detected | profile_data.py line 47-49 | Drop fully duplicated rows, keep first occurrence | `dropDuplicates()` | Step 2a |
| 2 | Datetime columns loaded as string (object) instead of timestamp, affecting 100% of rows | profile_data.py line 67 | Cast to TimestampType; drop rows with unparseable values | `F.to_timestamp()`, `.filter(isNotNull())` | Step 2b |
| 3 | passenger_count contains 0 values (physically impossible) | profile_data.py line 57-59 | Replace 0 with mode value (1), since ~70% of trips are single-passenger | `F.when().otherwise()` | Step 2c |
| 4 | fare_amount contains rows ≤ 0 (logically invalid — cancelled/test trips) | profile_data.py line 52-54 | Drop rows where fare_amount ≤ 0 | `.filter(col > 0)` | Step 2d |
| 5 | trip_distance > 100 miles (unrealistic for NYC taxis) | profile_data.py line 62-64 | Cap at 100 miles (winsorize); drop zero/negative distances | `F.when().otherwise()`, `.filter()` | Step 2e |
| 6 | store_and_fwd_flag contains invalid values (not Y or N) | profile_data.py line 70-72 | Replace invalid values with 'N' (dominant category) | `F.when().isin().otherwise()` | Step 2f |
| 7 | tip_amount, tolls_amount, improvement_surcharge, extra contain negative values | cleaning_strategy.md §8 | Replace negatives with 0.0 | `F.when(col < 0, 0.0).otherwise()` | Step 2g |
| 8 | Missing values detected across multiple columns | profile_data.py line 27 | Impute with mode/default values; drop nulls in critical columns | `.fillna()`, `.filter(isNotNull())` | Step 2h |
| 9 | total_amount may not match sum of components after cleaning | Derived during cleaning | Recalculate as sum of fare + extra + mta_tax + tip + tolls + surcharge | `F.col() + F.col() + ...` | Step 2i |
| 10 | Trip durations need derivation for analysis | Derived column | Compute (dropoff - pickup) in minutes; remove impossible durations (≤0 or >300 min) | `F.unix_timestamp()`, `F.round()` | Step 2i |
| 11 | Speed calculation needed for outlier detection | Derived column | Compute distance / (duration / 60); cap at 80 mph | `F.round()`, `F.when()` | Step 2i |
| 12 | Time-of-day categorization needed for analysis | Derived column | Flag as Morning/Afternoon/Evening/Night based on pickup_hour | `F.hour()`, `F.when()` | Step 2i |
| 13 | Payment type stored as integer code | Normalization | Map codes to labels: 1→Credit Card, 2→Cash, 3→No Charge, 4→Dispute | `F.when().otherwise()` | Step 2i |
| 14 | RatecodeID stored as integer code | Normalization | Map codes to labels: 1→Standard, 2→JFK, 3→Newark, etc. | `F.when().otherwise()` | Step 2i |

---

## How to Show During Demo

Run this command to show all A2 references at a glance:
```bash
grep -n "A2 Finding\|Action:" etl.py
```

This proves every transformation is traceable back to Assignment 2 profiling findings.
