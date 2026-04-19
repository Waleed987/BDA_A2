import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Base directory = wherever this script lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Force Pandas to show all columns and widen the display for the terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Load the dataset (First 2 million rows to easily exceed the 500k requirement)
file_path = os.path.join(BASE_DIR, "yellow_tripdata_2015-01.csv")
print("Loading dataset... (This might take a few seconds)")
df = pd.read_csv(file_path, nrows=2000000)

print("\n--- SCHEMA DESCRIPTION ---")
print(df.info())

print("\n--- MISSING VALUE ANALYSIS ---")
print(df.isnull().sum())

# Generate and save Missing Values Heatmap
plt.figure(figsize=(10,6))
sns.heatmap(df.isnull(), cbar=False, cmap='viridis')
plt.title('Missing Values Heatmap')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'missing_heatmap.png'))
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

print("\n--- DISTRIBUTION ANALYSIS (GENERATING 5 PLOTS) ---")

# 1. Fare Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['fare_amount'] > 0) & (df['fare_amount'] < 100)]['fare_amount'], bins=50, kde=True)
plt.title('Distribution of Fare Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_1_fare.png'))

# 2. Trip Distance
plt.figure(figsize=(8,5))
sns.histplot(df[(df['trip_distance'] > 0) & (df['trip_distance'] < 20)]['trip_distance'], bins=50, kde=True)
plt.title('Distribution of Trip Distance (Filtered 0-20 miles)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_2_distance.png'))

# 3. Tip Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['tip_amount'] >= 0) & (df['tip_amount'] < 20)]['tip_amount'], bins=50, kde=True)
plt.title('Distribution of Tip Amounts (Filtered 0-20)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_3_tip.png'))

# 4. Total Amount
plt.figure(figsize=(8,5))
sns.histplot(df[(df['total_amount'] > 0) & (df['total_amount'] < 100)]['total_amount'], bins=50, kde=True)
plt.title('Distribution of Total Amounts (Filtered 0-100)')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_4_total.png'))

# 5. Passenger Count
plt.figure(figsize=(8,5))
sns.histplot(df[(df['passenger_count'] >= 0) & (df['passenger_count'] <= 6)]['passenger_count'], discrete=True)
plt.title('Distribution of Passenger Count')
plt.tight_layout()
plt.savefig(os.path.join(BASE_DIR, 'dist_5_passengers.png'))

print("All tasks completed! Check your BDA_A2 folder for the 6 PNG files and copy the terminal output to your report.")