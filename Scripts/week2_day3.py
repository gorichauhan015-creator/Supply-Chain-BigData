from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("Week2_Day3").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("Data Loaded")
df.show(5)

# =====================================
# Fix datatypes
# =====================================

df = df.withColumn("shipping_real", col("Days for shipping (real)").cast("double"))
df = df.withColumn("sales_col", col("Sales").cast("double"))
df = df.withColumn("profit_col", col("Order profit per order").cast("double"))

# =====================================
# 1. Region wise avg delivery time
# =====================================

print("Average Delivery Time by Region:")
df.groupBy("Order region") \
  .agg(avg("shipping_real").alias("avg_delivery_time")) \
  .show()

# =====================================
# 2. Category wise total sales
# =====================================

print("Total Sales by Category:")
df.groupBy("Category name") \
  .agg(sum("sales_col").alias("total_sales")) \
  .show()

# =====================================
# 3. Order status count
# =====================================

print("Order Status Count:")
df.groupBy("Order status").count().show()

# =====================================
# 4. Shipping mode analysis
# =====================================

print("Average Delivery Time by Shipping Mode:")
df.groupBy("Shipping mode") \
  .agg(avg("shipping_real").alias("avg_delivery_time")) \
  .show()

# =====================================
# 5. Profit by region
# =====================================

print("Total Profit by Region:")
df.groupBy("Order region") \
  .agg(sum("profit_col").alias("total_profit")) \
  .show()

spark.stop()