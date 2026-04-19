import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# base directory so paths work no matter where script is run from
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# show all columns when printing dataframe
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# dataset path
CSV_FILE_PATH = os.path.join(BASE_DIR, "yellow_tripdata_2015-01.csv")

print("Loading dataset... (This might take a few seconds)")

# loading only first 2M rows (full dataset is too big)
df = pd.read_csv(CSV_FILE_PATH, nrows=2000000)

# ---------------- SCHEMA ----------------
print("\n--- SCHEMA DESCRIPTION ---")
print(df.info())  # shows columns, datatypes, non-null counts

# ---------------- MISSING VALUES ----------------
print("\n--- MISSING VALUE ANALYSIS ---")
print(df.isnull().sum())  # count missing values per column

# visual heatmap for missing values
plt.figure(figsize=(10,6))
sns.heatmap(df.isnull(), cbar=False, cmap='viridis')
plt.title('Missing Values Heatmap')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'missing_heatmap.png'))
print("-> missing_heatmap.png saved.")

# ---------------- STATISTICS ----------------
print("\n--- STATISTICAL SUMMARY ---")
print(df.describe().T.to_string())  # transpose for better readability

# ---------------- DATA QUALITY CHECKS ----------------
print("\n--- EXPLICIT DATA QUALITY ISSUES (COUNTS & PERCENTAGES) ---")

total_rows = len(df)

# duplicates
duplicate_rows = df.duplicated().sum()
duplicate_pct = (duplicate_rows / total_rows) * 100
print(f"1. Duplicates: {duplicate_rows} rows ({duplicate_pct:.4f}%)")

# invalid fare values (<= 0)
invalid_fare_count = df[df['fare_amount'] <= 0].shape[0]
invalid_fare_pct = (invalid_fare_count / total_rows) * 100
print(f"2. Outliers (Fare Amount <= 0): {invalid_fare_count} rows ({invalid_fare_pct:.4f}%)")

# zero passengers (not realistic)
zero_passenger_count = df[df['passenger_count'] == 0].shape[0]
zero_passenger_pct = (zero_passenger_count / total_rows) * 100
print(f"   Outliers (Passenger Count == 0): {zero_passenger_count} rows ({zero_passenger_pct:.4f}%)")

# extreme distances (>100 miles is suspicious)
extreme_distance_count = df[df['trip_distance'] > 100].shape[0]
extreme_distance_pct = (extreme_distance_count / total_rows) * 100
print(f"   Outliers (Distance > 100 miles): {extreme_distance_count} rows ({extreme_distance_pct:.4f}%)")

# datetime loaded as string (common issue in CSVs)
print(f"3. Type Inconsistencies: 'tpep_pickup_datetime' and 'tpep_dropoff_datetime' loaded as string instead of datetime. Affects {total_rows} rows (100%).")

# invalid flag values (should only be Y or N)
invalid_flag_count = df[~df['store_and_fwd_flag'].isin(['Y', 'N'])].shape[0]
invalid_flag_pct = (invalid_flag_count / total_rows) * 100
print(f"4. Formatting Errors (Invalid store_and_fwd_flag): {invalid_flag_count} rows ({invalid_flag_pct:.4f}%)")

# ---------------- DISTRIBUTION PLOTS ----------------
print("\n--- DISTRIBUTION ANALYSIS (GENERATING 5 PLOTS) ---")

# fare distribution (filtered to remove extreme outliers)
plt.figure(figsize=(8,5))
sns.histplot(df[(df['fare_amount'] > 0) & (df['fare_amount'] < 100)]['fare_amount'], bins=50, kde=True)
plt.title('Distribution of Fare Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_1_fare.png'))

# trip distance distribution
plt.figure(figsize=(8,5))
sns.histplot(df[(df['trip_distance'] > 0) & (df['trip_distance'] < 20)]['trip_distance'], bins=50, kde=True)
plt.title('Distribution of Trip Distance (Filtered 0-20 miles)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_2_distance.png'))

# tip amount distribution
plt.figure(figsize=(8,5))
sns.histplot(df[(df['tip_amount'] >= 0) & (df['tip_amount'] < 20)]['tip_amount'], bins=50, kde=True)
plt.title('Distribution of Tip Amounts (Filtered 0-20)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_3_tip.png'))

# total amount distribution
plt.figure(figsize=(8,5))
sns.histplot(df[(df['total_amount'] > 0) & (df['total_amount'] < 100)]['total_amount'], bins=50, kde=True)
plt.title('Distribution of Total Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_4_total.png'))

# passenger count distribution (discrete values)
plt.figure(figsize=(8,5))
sns.histplot(df[(df['passenger_count'] >= 0) & (df['passenger_count'] <= 6)]['passenger_count'], discrete=True)
plt.title('Distribution of Passenger Count')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_5_passengers.png'))

print("All tasks completed! Check your folder for the PNG files and copy output to report.")