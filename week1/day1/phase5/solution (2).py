orders = spark.read.csv('/Volumes/workspace/default/my_volume/FileStore/olist/olist_orders_dataset.csv', header=True, inferSchema=True)
customers = spark.read.csv('/Volumes/workspace/default/my_volume/FileStore/olist/olist_customers_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('/Volumes/workspace/default/my_volume/FileStore/olist/olist_order_items_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('/Volumes/workspace/default/my_volume/FileStore/olist/olist_products_dataset.csv', header=True, inferSchema=True)
payments = spark.read.csv('/Volumes/workspace/default/my_volume/FileStore/olist/olist_order_payments_dataset.csv', header=True, inferSchema=True)

# Task 1: Top 3 Customers per City Calculate total spend per customer, then use window function to rank customers within each city. Output: city, customer_id, total_spend, rank
from pyspark.sql.functions import sum
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

df = customers.join(orders, "customer_id").join(payments, "order_id")
customer_spend = df.groupBy("customer_city", "customer_id").agg(sum("payment_value").alias("total_spend"))
windowSpec = Window.partitionBy("customer_city").orderBy(customer_spend["total_spend"].desc())

result = customer_spend.withColumn("rank", rank().over(windowSpec)).filter("rank <= 3")
display(result)

#Task 2: Running Total of Sales Calculate daily sales and then cumulative (running) total using windowfunction. Output: date, daily_sales, running_total
from pyspark.sql.functions import to_date, sum
from pyspark.sql.window import Window

sales = payments.join(orders, "order_id").withColumn("date", to_date("order_purchase_timestamp"))

daily_sales = sales.groupBy("date").agg(sum("payment_value").alias("daily_sales"))

windowSpec = Window.orderBy("date")

result = daily_sales.withColumn("running_total",sum("daily_sales").over(windowSpec))

display(result)

# Task 3: Top Products per Category Aggregate sales per product, join with category, then rank using DENSE_RANK. Output: category, product_id, total_sales, rank

from pyspark.sql.functions import dense_rank

df = order_items.join(products, "product_id")

product_sales = df.groupBy("product_category_name", "product_id") .agg(sum("price").alias("total_sales"))

windowSpec = Window.partitionBy("product_category_name").orderBy(product_sales["total_sales"].desc())

result = product_sales.withColumn("rank", dense_rank().over(windowSpec))

display(result)

#Task 4: Customer Lifetime Value Calculate total spend per customer across all orders. Output: customer_id, total_spend

clv = payments.join(orders, "order_id").groupBy("customer_id").agg(sum("payment_value").alias("total_spend"))

display(clv)

# Task 5: Customer Segmentation Apply logic: total_spend > 10000 → Gold 5000–10000 → Silver <5000
# → Bronze Add a new column 'segment'. Then group by segment to count customers. Output:
# customer_id, total_spend, segment

from pyspark.sql.functions import when

segmented = clv.withColumn("segment",
    when(clv.total_spend > 10000, "Gold").when((clv.total_spend >= 5000) & (clv.total_spend <= 10000), "Silver").otherwise("Bronze")
)

display(segmented)

# Count per segment
segment_count = segmented.groupBy("segment").count()
display(segment_count)

# Task 6: Final Reporting Table Combine results into a single dataset containing: customer_id, city,
# total_spend, segment, total_orders
orders_count = orders.groupBy("customer_id").count().withColumnRenamed("count", "total_orders")

final_df = segmented.join(customers, "customer_id").join(orders_count, "customer_id").select("customer_id", "customer_city", "total_spend", "segment", "total_orders")

