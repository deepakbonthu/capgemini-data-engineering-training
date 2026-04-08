from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

spark = SparkSession.builder.appName("Phase2").getOrCreate()

# -------------------------
# Customers Data
# -------------------------
customers_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bengaluru", 35),
    (5, "Kiran", "Chennai", 22)
]

customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name", "city", "age"]
)

# -------------------------
# Orders Data
# -------------------------
orders_data = [
    (101, 1, 2500),
    (102, 2, 1800),
    (103, 1, 3200),
    (104, 3, 1500),
    (105, 5, 2800)
]

orders = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "amount"]
)

# -------------------------
# Show Data
# -------------------------
customers.show()
orders.show()

# -------------------------
# 1. Total amount per customer
# -------------------------
customers.join(orders, "customer_id") \
.groupBy("customer_id") \
.sum("amount") \
.show()

# -------------------------
# 2. Top 3 customers by spend
# -------------------------
orders.groupBy("customer_id") \
.sum("amount") \
.orderBy("sum(amount)", ascending=False) \
.limit(3) \
.show()

# -------------------------
# 3. Customers with no orders
# -------------------------
customers.join(orders, "customer_id", "left_anti") \
.show()

# -------------------------
# 4. City-wise total revenue
# -------------------------
customers.join(orders, "customer_id") \
.groupBy("city") \
.sum("amount") \
.show()

# -------------------------
# 5. Average order amount per customer
# -------------------------
orders.groupBy("customer_id") \
.agg(avg("amount").alias("avg_amount")) \
.show()

# -------------------------
# 6. Customers with more than 1 order
# -------------------------
orders.groupBy("customer_id") \
.agg(count("*").alias("order_count")) \
.filter("order_count > 1") \
.show()

# -------------------------
# 7. Sort customers by total spend
# -------------------------
orders.groupBy("customer_id") \
.sum("amount") \
.orderBy("sum(amount)", ascending=False) \
.show()Added PySpark file for Phase 2