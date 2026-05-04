from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round, count

spark = SparkSession.builder.appName("Week3_Day3").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

# Fix datatypes
df = df.withColumn("sales_col", col("Sales").cast("double"))
df = df.withColumn("profit_col", col("Order profit per order").cast("double"))

# ============================
# AGGREGATIONS
# ============================

region_sales = df.groupBy("Order region") \
    .agg(round(sum("sales_col"), 0).alias("total_sales"))

profit_region = df.groupBy("Order region") \
    .agg(round(sum("profit_col"), 0).alias("total_profit"))

orders_region = df.groupBy("Order region") \
    .agg(count("Order id").alias("total_orders"))

# ============================
# JOIN ALL (FINAL DATASET)
# ============================

final_df = region_sales \
    .join(profit_region, "Order region") \
    .join(orders_region, "Order region")

print("Final Dashboard Dataset:")
final_df.show()

# ============================
# SAVE FINAL DATA
# ============================

final_df.write.mode("overwrite").csv("output/final_dashboard_data/")

print("Week 3 Day 3 Completed - Final dataset ready for Power BI")

spark.stop()