# ---------- IMPORTS ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, concat_ws

# ---------- SPARK SESSION ----------
spark = SparkSession.builder.appName("phase4_mini_project").getOrCreate()

# ---------- LOAD DATA ----------
sales_df = spark.read.csv('/samples/sales.csv', header=True, inferSchema=True)
customers_df = spark.read.csv('/samples/customers.csv', header=True, inferSchema=True)

# ---------- DATA CLEANING ----------
sales_df = sales_df.dropna(subset=["customer_id"])
customers_df = customers_df.dropna(subset=["customer_id"])

sales_df = sales_df.dropDuplicates()
customers_df = customers_df.dropDuplicates()

sales_df = sales_df.filter(col("total_amount") > 0)


# =================================================
# 1. DAILY SALES
# =================================================
daily_sales = sales_df.groupBy("sale_date").agg(sum("total_amount").alias("total_sales"))

print("========= DAILY SALES =========")
daily_sales.show()


# =================================================
# 2. CITY-WISE REVENUE
# =================================================
data = customers_df.join(sales_df, "customer_id")

city_wise_revenue = data.groupBy("city").agg(sum("total_amount").alias("revenue"))
print("========= CITY REVENUE =========")
city_wise_revenue.show()


# =================================================
# 3. TOP 5 CUSTOMERS
# =================================================
top_customers = data.groupBy("customer_id").agg(sum("total_amount").alias("total_spend")).orderBy(col("total_spend").desc()).limit(5)

print("========= TOP 5 CUSTOMERS =========")
top_customers.show()

# =================================================
# 4. REPEAT CUSTOMERS (>1 ORDER)
# =================================================
repeat_customers = sales_df.groupBy("customer_id").agg(count("*").alias("order_count")).filter(col("order_count") > 1)

print("========= REPEAT CUSTOMERS =========")
repeat_customers.show()

# =================================================
# 5. CUSTOMER SEGMENTATION
# =================================================
customer_spend = data.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spend"))

customer_segmentation = customer_spend.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

print("========= CUSTOMER SEGMENTATION =========")
customer_segmentation.show()

# =================================================
# 6. FINAL REPORT
# =================================================
customer_name = customers_df.withColumn(
    "customer_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

final_table = customer_name.join(customer_segmentation, "customer_id", "left").join(repeat_customers, "customer_id", "left").select("customer_name", "city", "total_spend", "order_count", "segment")

print("========= FINAL REPORT =========")
final_table.show()

# =================================================
# 7. SAVE OUTPUT
# =================================================
final_table.write.mode("overwrite").option("header", "true").csv('/tmp/output/report')
