# Phase 3A – Data Cleaning Pipeline using PySpark and MySQL

## Objective
The goal of this project is to clean messy data using PySpark and implement the same logic using MySQL.

Data cleaning ensures accuracy, consistency, and reliability before performing analysis or building pipelines.

---

## Dataset Issues

- Missing values in `customer_id`, `name`, `city`
- Duplicate records
- Invalid age values (negative numbers)
- Incomplete customer information

---

## Data Cleaning Rules Applied

- Removed rows where `customer_id` is NULL
- Replaced NULL values in `name` with **"Unknown"**
- Replaced NULL values in `city` with **"Unknown"**
- Removed duplicate records
- Removed rows where age is negative

---

## PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CustomerSales").getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Row count before cleaning:", df.count())

df_clean = df.filter(col("customer_id").isNotNull())

df_clean = df_clean.fillna({
"name": "Unknown",
"city": "Unknown"
})

df_clean = df_clean.dropDuplicates()

df_clean = df_clean.filter(col("age") >= 0)

print("Cleaned Data")
df_clean.show()

print("Row count after cleaning:", df_clean.count())
