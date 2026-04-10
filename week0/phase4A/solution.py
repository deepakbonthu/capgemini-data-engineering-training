from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("phase4").getOrCreate()

customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
sales.show()

cs_joined = customers.join(sales, "customer_id")
cs_joined= cs_joined.withColumn("customer_name", concat_ws(" ", "first_name", "last_name"))

# 1. Create Gold/Silver/Bronze segmentation using conditional logic
conditional_seg = cs_joined.groupBy("customer_id", "customer_name", "city") \
  .agg(sum("total_amount").alias("total_spend"), 
       count("sale_id").alias("order_count")
      )
conditional_seg = conditional_seg.withColumn(
  "segment", 
  when(col("total_spend") >100 , "Gold")
  .when((col("total_spend") >= 50) & (col("total_spend") <=100) , "Silver") 
  .otherwise("Bronze")
)

conditional_seg.show()

# 2. Group data by segment and count customers
conditional_seg.groupBy("segment").count().show()

# 3. Try quantile-based segmentation
quantiles = conditional_seg.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

# Apply segmentation
quantile_seg = conditional_seg.withColumn(
    "quantile_segment",
    when(col("total_spend") <= q1, "Low")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Medium")
    .otherwise("High")
)

quantile_seg.select(
    "customer_id", "customer_name", "city",
    "total_spend", "order_count", "quantile_segment"
).show()

# 4. Compare results of different methods
# task 4-1 Bucketizer (MLlib)
from pyspark.ml.feature import Bucketizer

splits = [-float("inf"), 50, 100, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket_segment"
)

bucket_seg = bucketizer.transform(conditional_seg)
bucket_seg.select(
    "customer_id", "customer_name", "city",
    "total_spend", "order_count", "bucket_segment"
).show()

# task 4-2 Window-based Ranking
window = Window.orderBy("total_spend")

rank_seg = conditional_seg.withColumn(
    "window_segment",
    percent_rank().over(window)
)
rank_seg.select(
    "customer_id", "customer_name", "city",
    "total_spend", "order_count", "window_segment"
).show()
