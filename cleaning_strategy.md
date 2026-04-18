# Proposed Cleaning Strategy

For every data quality issue identified during profiling, the following specific and justified cleaning actions will be applied in Assignment 3.

---

## 1. Duplicate Rows

**Issue:** Duplicate rows detected in the dataset.

**Action:** Drop all fully duplicated rows using `df.drop_duplicates()`, keeping the first occurrence. Full row duplication across all 19 columns indicates repeated data entry or ingestion errors rather than legitimate repeated trips, as each trip has unique pickup/dropoff timestamps.

---

## 2. Negative/Zero Fare Amounts

**Issue:** `fare_amount` contains rows with values ≤ 0, which are logically invalid since every taxi trip must have a positive fare.

**Action:** Replace negative fare values with the median fare amount for the same `trip_distance` bucket (rounded to nearest mile), as the median is robust against outliers in this right-skewed distribution. Zero fares will be dropped entirely, as they likely represent cancelled or test trips with no legitimate transaction.

---

## 3. Zero Passenger Count

**Issue:** `passenger_count` contains rows with a value of 0, which is physically impossible for a completed trip.

**Action:** Replace 0 values with the mode of `passenger_count` (which is 1), since single-passenger trips dominate the dataset (~70%) and a zero count most likely results from a driver failing to enter the count. Dropping these rows would lose valid trip and fare data unnecessarily.

---

## 4. Extreme Trip Distances (> 100 miles)

**Issue:** `trip_distance` contains values exceeding 100 miles, which are unrealistic for NYC yellow taxi trips that primarily operate within the five boroughs.

**Action:** Cap trip distances at the 99.9th percentile value using winsorization. Trips beyond 100 miles are almost certainly GPS errors or meter malfunctions. The 99.9th percentile threshold preserves legitimate long trips (e.g., JFK to distant suburbs) while removing impossible values such as 500+ mile entries.

---

## 5. Type Inconsistencies — Datetime Columns

**Issue:** `tpep_pickup_datetime` and `tpep_dropoff_datetime` are loaded as `object` (string) type instead of `datetime64`, affecting 100% of rows. This prevents time-based analysis, trip duration computation, and temporal aggregations.

**Action:** Convert both columns to `datetime64` using `pd.to_datetime(df['column'], errors='coerce')`. The `errors='coerce'` flag will turn unparseable strings into `NaT` instead of raising exceptions, allowing us to identify and count any malformed datetime entries. Rows with `NaT` after conversion (if any) will be dropped since trip timing is essential for downstream analytics.

---

## 6. Invalid `store_and_fwd_flag` Values

**Issue:** `store_and_fwd_flag` should only contain 'Y' (store and forward) or 'N' (not store and forward). Any values outside this set represent formatting or encoding errors.

**Action:** Replace any invalid values with 'N' (the dominant category), since the vast majority of trips are transmitted in real-time. This flag has minimal impact on fare or distance analysis, so imputation with the mode is a low-risk approach that preserves row completeness.

---

## 7. Missing Values (Nulls)

**Issue:** Several columns contain null values that could skew aggregations and break downstream computations.

**Action per column:**

| Column | Action | Justification |
|--------|--------|---------------|
| `passenger_count` | Impute with mode (1) | Most trips are single-passenger; nulls likely represent missed entries |
| `trip_distance` | Drop rows with null distance | Distance is a critical dimension; imputation would fabricate trip data |
| `fare_amount` | Drop rows with null fare | Fare is the primary metric; imputed values would corrupt revenue analysis |
| `RatecodeID` | Impute with mode (1 = Standard rate) | Standard rate dominates; missing codes most likely standard trips |
| `store_and_fwd_flag` | Impute with 'N' | Dominant category; low analytical impact |
| `payment_type` | Impute with mode (1 = Credit card) | Missing payment type doesn't affect trip-level analysis significantly |

---

## 8. Negative Tip / Tolls / Surcharge Amounts

**Issue:** `tip_amount`, `tolls_amount`, `improvement_surcharge`, and `extra` may contain negative values, which are invalid for additive fare components.

**Action:** Replace negative values with 0 for all additive fare component columns. Negative surcharges, tips, and tolls have no legitimate meaning and are data entry errors. Setting to 0 (rather than dropping the row) preserves the trip record while correcting only the erroneous field.

---

## Summary

| Issue | Count/Scope | Action | Risk Level |
|-------|------------|--------|------------|
| Duplicates | Identified count | Drop duplicates (keep first) | Low |
| Fare ≤ 0 | Identified count | Median imputation by distance bucket / drop zeros | Medium |
| Passenger = 0 | Identified count | Impute with mode (1) | Low |
| Distance > 100 mi | Identified count | Cap at 99.9th percentile | Low |
| Datetime as string | 100% of rows | `pd.to_datetime` with `errors='coerce'` | Low |
| Invalid flag values | Identified count | Replace with 'N' | Low |
| Nulls | Varies by column | Column-specific impute or drop (see table above) | Medium |
| Negative fare components | Identified count | Replace with 0 | Low |
