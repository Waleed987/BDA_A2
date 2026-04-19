Here’s a cleaner rephrase in a more natural, student-style tone—like something you’d realistically write:

---

# Task 1: Dataset Selection & Justification

## Dataset Description

**Name:** NYC Yellow Taxi Trip Data (January 2015 & January–March 2016)

**Source:** Kaggle — NYC Yellow Taxi Trip Data dataset by Elemento. The data was originally collected and published by the New York City Taxi & Limousine Commission (TLC) under the U.S. Government Works license.

**Contents:**
This dataset contains detailed records of taxi trips in New York City. Each row represents a single ride and includes information like pickup and dropoff time, GPS coordinates, trip distance, number of passengers, fare breakdown (fare, tips, tolls, etc.), payment type, and rate code.

For this assignment, the file `yellow_tripdata_2015-01.csv` is used, which contains around **12.7 million records** and **19 attributes**, making it quite large and realistic for data processing tasks.

**Attributes (19 columns):**
The dataset includes a mix of fields such as vendor ID, pickup/dropoff timestamps, passenger count, trip distance, coordinates, payment type, and different fare components. These columns cover both trip details and financial information for each ride.

---

## Warehouse Suitability

This dataset is a very good fit for a data warehouse project for several reasons:

1. **Large data size:**
   With over 12 million records (~1.9 GB), the dataset is big enough to require distributed storage and processing (like HDFS and Spark), instead of simple local tools.

2. **Easy to model as a star schema:**
   The data can be structured into a fact table (`fact_trips`) and multiple dimension tables such as datetime, location, vendor, rate code, and payment type. This makes it ideal for warehouse design.

3. **Variety of data types:**
   It includes numeric, categorical, string, and datetime fields, which helps in handling real-world data complexity.

4. **Real-world data issues:**
   The dataset isn’t perfectly clean — it includes problems like negative fares, zero passengers, and extreme values. This makes data cleaning and preprocessing more meaningful.

5. **Reliable source:**
   Since the data comes from a government authority (NYC TLC), it is trustworthy and widely used in research and industry.

---

## Business Questions

Using this dataset in a data warehouse, we can answer several useful business questions:

1. **Peak revenue hours:**
   When do taxis earn the most, and how do fares compare between rush hours and off-peak times?

2. **High-revenue locations:**
   Which pickup areas generate the most revenue, and how does this change between weekdays and weekends?

3. **Payment trends:**
   What is the split between cash and card payments, and do card payments result in higher tips?

4. **Rate code analysis:**
   How do trip distance and fares vary across different rate types (standard, JFK, Newark, etc.), and which ones contribute most to revenue?

5. **Data quality issues:**
   What percentage of trips contain anomalies (like zero passengers or negative fares), and are these linked to specific vendors or time periods?

---

If you want, I can make it sound even more “casual NUML/NUST student style” or slightly more polished depending on your professor’s expectations.
