from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("Week2_Day3").getOrCreate()

#  Load cleaned data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("Data Loaded")
df.show(5)

# =====================================
#  1. REGION WISE AVG DELIVERY TIME
# =====================================

df = df.withColumn(
    "shipping_real",
    col("Days for shipping (real)").cast("double")
)

print("Average Delivery Time by Region:")
df.groupBy("Order region") \
  .agg(avg("shipping_real").alias("avg_delivery_time")) \
  .show()

# =====================================
#  2. CATEGORY WISE TOTAL SALES
# =====================================

df = df.withColumn(
    "sales_col",
    col("Sales").cast("double")
)

print("Total Sales by Category:")
df.groupBy("Category name") \
  .agg(sum("sales_col").alias("total_sales")) \
  .show()

# =====================================
#  3. ORDER STATUS COUNT
# =====================================

print("Order Status Count:")
df.groupBy("Order status").count().show()

spark.stop()