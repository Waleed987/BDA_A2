import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Force Pandas to show all columns and widen the display for the terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Load the dataset (First 2 million rows to easily exceed the 500k requirement)
file_path = "/mnt/c/Users/pc/Desktop/BDA_A2/yellow_tripdata_2015-01.csv"
print("Loading dataset... (This might take a few seconds)")
df = pd.read_csv(file_path, nrows=2000000)

print("\n--- SCHEMA DESCRIPTION ---")
print(df.info())

print("\n--- MISSING VALUE ANALYSIS ---")
missing_counts = df.isnull().sum()
missing_pct = (df.isnull().sum() / len(df)) * 100
missing_df = pd.DataFrame({'Missing Count': missing_counts, 'Missing %': missing_pct.round(4)})
print(missing_df.to_string())

# Generate and save Missing Values Heatmap
plt.figure(figsize=(10,6))
sns.heatmap(df.isnull(), cbar=False, cmap='viridis')
plt.title('Missing Values Heatmap')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/missing_heatmap.png')
print("-> missing_heatmap.png saved.")

print("\n--- STATISTICAL SUMMARY ---")
# .T transposes the table to show all attributes down the left side
print(df.describe().T.to_string())

print("\n--- EXPLICIT DATA QUALITY ISSUES (COUNTS & PERCENTAGES) ---")
total_rows = len(df)

# 1. Duplicates
duplicates = df.duplicated().sum()
dup_pct = (duplicates / total_rows) * 100
print(f"1. Duplicates: {duplicates} rows ({dup_pct:.4f}%)")

# 2. Outliers (Logical Anomalies)
invalid_fares = df[df['fare_amount'] <= 0].shape[0]
fare_pct = (invalid_fares / total_rows) * 100
print(f"2. Outliers (Fare Amount <= 0): {invalid_fares} rows ({fare_pct:.4f}%)")

zero_passengers = df[df['passenger_count'] == 0].shape[0]
pass_pct = (zero_passengers / total_rows) * 100
print(f"   Outliers (Passenger Count == 0): {zero_passengers} rows ({pass_pct:.4f}%)")

extreme_dist = df[df['trip_distance'] > 100].shape[0]
dist_pct = (extreme_dist / total_rows) * 100
print(f"   Outliers (Distance > 100 miles): {extreme_dist} rows ({dist_pct:.4f}%)")

# 3. Type Inconsistencies
print(f"3. Type Inconsistencies: 'tpep_pickup_datetime' and 'tpep_dropoff_datetime' loaded as 'str/object' instead of native 'datetime64'. Affects {total_rows} rows (100.00%).")

# 4. Formatting Errors
invalid_flag = df[~df['store_and_fwd_flag'].isin(['Y', 'N'])].shape[0]
flag_pct = (invalid_flag / total_rows) * 100
print(f"4. Formatting Errors (Invalid store_and_fwd_flag): {invalid_flag} rows ({flag_pct:.4f}%)")

neg_tip = df[df['tip_amount'] < 0].shape[0]
neg_tip_pct = (neg_tip / total_rows) * 100
print(f"   Outliers (Tip Amount < 0): {neg_tip} rows ({neg_tip_pct:.4f}%)")

neg_total = df[df['total_amount'] <= 0].shape[0]
neg_total_pct = (neg_total / total_rows) * 100
print(f"   Outliers (Total Amount <= 0): {neg_total} rows ({neg_total_pct:.4f}%)")

print("\n--- PROPOSED CLEANING STRATEGY ---")
print("""
1. NEGATIVE / ZERO FARE AMOUNTS
   Issue  : Column 'fare_amount' has """ + str(invalid_fares) + f""" rows with values <= 0 ({fare_pct:.4f}% of records).
   Cause  : Data entry errors or system glitches during payment processing.
   Action : Replace negative/zero fare values with the median fare amount for the same
            RateCodeID and comparable trip_distance bucket (rounded to nearest mile),
            as the median is robust against outliers in this right-skewed distribution.

2. ZERO PASSENGER COUNT
   Issue  : Column 'passenger_count' has """ + str(zero_passengers) + f""" rows with value == 0 ({pass_pct:.4f}% of records).
   Cause  : Driver failed to enter passenger count or meter default was not updated.
   Action : Replace 0 values with the mode (most frequent value = 1) for the same VendorID,
            since solo rides dominate the dataset and mode preserves the true distribution.

3. EXTREME TRIP DISTANCES
   Issue  : Column 'trip_distance' has """ + str(extreme_dist) + f""" rows with values > 100 miles ({dist_pct:.4f}% of records).
   Cause  : GPS errors or metering malfunctions producing unrealistically long distances.
   Action : Cap trip_distance at the 99.9th percentile value. Rows exceeding this threshold
            will have trip_distance replaced with the 99.9th percentile value, preserving the
            record while removing the distortion from downstream aggregations.

4. DATETIME TYPE INCONSISTENCY
   Issue  : Columns 'tpep_pickup_datetime' and 'tpep_dropoff_datetime' are loaded as 'object'
            (string) instead of 'datetime64'. Affects {total_rows} rows (100.00%).
   Cause  : pandas read_csv does not auto-parse datetime columns without explicit instruction.
   Action : Convert both columns using pd.to_datetime() with format='%Y-%m-%d %H:%M:%S'.
            Rows that fail parsing (invalid dates) will be coerced to NaT and subsequently
            dropped, as pickup/dropoff time is essential for any time-based analysis.

5. DUPLICATE ROWS
   Issue  : """ + str(duplicates) + f""" fully duplicate rows detected ({dup_pct:.4f}% of records).
   Cause  : Possible re-transmission of trip records from vendor systems.
   Action : Drop exact duplicate rows using df.drop_duplicates(), keeping the first occurrence.
            Since these are full-row duplicates, no information is lost.

6. INVALID store_and_fwd_flag VALUES
   Issue  : Column 'store_and_fwd_flag' has """ + str(invalid_flag) + f""" rows with values
            outside the expected domain {{Y, N}} ({flag_pct:.4f}% of records).
   Cause  : Encoding inconsistencies or corrupted records.
   Action : Map any non-standard values to 'N' (the dominant category), as the flag
            indicates whether the trip was stored locally before forwarding — defaulting
            to 'N' (not stored) is the conservative and safe assumption.

7. NEGATIVE TIP AMOUNTS
   Issue  : Column 'tip_amount' has """ + str(neg_tip) + f""" rows with values < 0 ({neg_tip_pct:.4f}% of records).
   Cause  : Reversed transactions or data entry errors.
   Action : Replace negative tip values with 0.00, as tips cannot logically be negative.
            Setting to zero rather than median avoids artificially inflating tip averages.

8. NEGATIVE / ZERO TOTAL AMOUNTS
   Issue  : Column 'total_amount' has """ + str(neg_total) + f""" rows with values <= 0 ({neg_total_pct:.4f}% of records).
   Cause  : Voided trips, refunds, or system errors.
   Action : Recalculate total_amount as the sum of fare_amount + extra + mta_tax + tip_amount
            + tolls_amount + improvement_surcharge (after the above cleaning steps). If the
            recalculated total is still <= 0, drop the row as it represents an invalid trip.
""")

print("\n--- DISTRIBUTION ANALYSIS (GENERATING 5 PLOTS) ---")

# 1. Fare Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['fare_amount'] > 0) & (df['fare_amount'] < 100)]['fare_amount'], bins=50, kde=True)
plt.title('Distribution of Fare Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/dist_1_fare.png')

# 2. Trip Distance
plt.figure(figsize=(8,5))
sns.histplot(df[(df['trip_distance'] > 0) & (df['trip_distance'] < 20)]['trip_distance'], bins=50, kde=True)
plt.title('Distribution of Trip Distance (Filtered 0-20 miles)')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/dist_2_distance.png')

# 3. Tip Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['tip_amount'] >= 0) & (df['tip_amount'] < 20)]['tip_amount'], bins=50, kde=True)
plt.title('Distribution of Tip Amounts (Filtered 0-20)')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/dist_3_tip.png')

# 4. Total Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['total_amount'] > 0) & (df['total_amount'] < 100)]['total_amount'], bins=50, kde=True)
plt.title('Distribution of Total Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/dist_4_total.png')

# 5. Passenger Count
plt.figure(figsize=(8,5))
sns.histplot(df[(df['passenger_count'] >= 0) & (df['passenger_count'] <= 6)]['passenger_count'], discrete=True)
plt.title('Distribution of Passenger Count')
plt.tight_layout()
plt.savefig('/mnt/c/Users/pc/Desktop/BDA_A2/dist_5_passengers.png')

print("All tasks completed! Check your BDA_A2 folder for the 6 PNG files and copy the terminal output to your report.")