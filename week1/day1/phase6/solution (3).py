from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# =========================
# Dirty Customers Dataset
# =========================

customers_data = [
    (1, "John Doe", "john@example.com", "Hyderabad"),
    (2, "Alice ", "alice@example.com", "Chennai"),
    (3, None, "bob@example.com", "Bangalore"),        # NULL name
    (4, "David", None, "Mumbai"),                    # NULL email
    (5, "Eva", "eva@example.com", "Hyderabad"),
    (6, "Frank", "frank@example.com", "Delhi"),
]

customers = spark.createDataFrame(customers_data, ["customer_id", "name", "email", "city"])

# =========================
# Dirty Orders Dataset
# =========================

orders_data = [
    (101, 1, "2024-01-01", 1000),
    (102, 2, "2024-01-02", 2000),
    (103, 3, "2024-01-03", -500),     # INVALID negative value
    (104, 99, "2024-01-04", 1500),    # INVALID FK (customer_id 99)
    (105, 1, "2024-01-05", None),     # NULL amount
    (106, 5, "2024-01-06", 3000),
    (107, 5, "2024-01-07", 3000),     # duplicate-like record
]

orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_date", "amount"])

# =========================
# Convert date column
# =========================

orders = orders.withColumn("order_date", to_date(col("order_date")))
# TODO 1: Clean data
# - Remove nulls
# - Handle negative values
# - Trim names

# Remove nulls and clean customers
customers_clean = customers \
    .dropna(subset=["customer_id", "name", "email"]) \
    .withColumn("name", trim(col("name")))
customers_clean.show()

# Clean orders
orders_clean = orders \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") >= 0) \
    .dropDuplicates()
orders_clean.show()
# TODO 2: Validate data
# - Find invalid customer_id using left_anti join
invalid_orders = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left_anti"
)

invalid_orders.show()
# TODO 3: Join datasets
joined_df = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="inner"
)
joined_df.show()
# TODO 4: Apply transformations
# - total spend per customer
# - count orders
agg_df = joined_df.groupBy("customer_id", "name") \
    .agg(
        sum("amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )
agg_df.show()
# TODO 5: Window functions
# - rank customers by spend
from pyspark.sql.window import Window

window_spec = Window.orderBy(col("total_spend").desc())

final_df = agg_df.withColumn(
    "rank",
    rank().over(window_spec)
)
final_df.show()
# TODO 6: Save output
# final_df.write.mode("overwrite").csv("/tmp/phase6_output")
final_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/my_volume/phase6_output")
final_df.show()
