from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("practice").getOrCreate()

customers_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bengaluru", 35),
    (5, "Kiran", "Chennai", 22)
]

customers = spark.createDataFrame(customers_data, ["customer_id","customer_name","city","age"])

customers.show()

customers.filter(customers.city == "Chennai").show()

customers.filter(customers.age > 25).show()

customers.select("customer_name", "city").show()

customers.groupBy("city").agg(count("*").alias("total_customers")).show()
