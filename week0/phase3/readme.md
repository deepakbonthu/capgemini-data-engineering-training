from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.appName("Phase3_ETL").getOrCreate()

# -------------------------
# Extract (Raw Data)
# -------------------------
data = [
    (1, "Ravi", "Hyderabad", 2500),
    (2, "Sita", "Chennai", None),
    (3, "Arun", "Hyderabad", 3000),
    (4, "Meena", "Bengaluru", 0),
    (5, "Kiran", "Chennai", 2800),
    (1, "Ravi", "Hyderabad", 1500)
]

df = spark.createDataFrame(data, ["customer_id", "name", "city", "amount"])

df.show()

# -------------------------
# Transform (Cleaning)
# -------------------------
clean_df = df.dropna()

filtered_df = clean_df.filter(clean_df.amount > 0)

filtered_df.show()

# -------------------------
# Business Logic
# -------------------------

# 1. City-wise revenue
city_revenue = filtered_df.groupBy("city").sum("amount")
city_revenue.show()

# 2. Repeat customers (>1 order)
repeat_customers = filtered_df.groupBy("customer_id") \
.agg(count("*").alias("order_count")) \
.filter("order_count > 1")

repeat_customers.show()

# -------------------------
# Load (Final Output)
# -------------------------
final_df = filtered_df.groupBy("customer_id") \
.agg(sum("amount").alias("total_spend"))

final_df.show()
