from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round, count

spark = SparkSession.builder.appName("Week3_Day2").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

# Fix datatypes
df = df.withColumn("sales_col", col("Sales").cast("double"))
df = df.withColumn("profit_col", col("Order profit per order").cast("double"))

# ============================
# 1. PROFIT BY REGION
# ============================
profit_region = df.groupBy("Order region") \
    .agg(round(sum("profit_col"), 0).alias("total_profit"))

print("Profit by Region:")
profit_region.show()

# ============================
# 2. TOTAL ORDERS BY REGION
# ============================
orders_region = df.groupBy("Order region") \
    .agg(count("Order id").alias("total_orders"))

print("Orders by Region:")
orders_region.show()

# ============================
# 3. LATE DELIVERY RISK COUNT
# ============================
late_delivery = df.groupBy("Late_delivery_risk") \
    .agg(count("*").alias("total_orders"))

print("Late Delivery Risk:")
late_delivery.show()

# ============================
# SAVE OUTPUT
# ============================
profit_region.write.mode("overwrite").csv("output/profit_region/")
orders_region.write.mode("overwrite").csv("output/orders_region/")
late_delivery.write.mode("overwrite").csv("output/late_delivery/")

print("Week 3 Day 2 Completed")

spark.stop()