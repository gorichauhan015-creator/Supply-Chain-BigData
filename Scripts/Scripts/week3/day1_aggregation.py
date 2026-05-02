from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round

spark = SparkSession.builder.appName("Week3_Day1").getOrCreate()

# Load Data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

# Fix Data Type
df = df.withColumn("sales_col", col("Sales").cast("double"))

# ============================
# 1. REGION SALES
# ============================
region_sales = df.groupBy("Order region") \
    .agg(round(sum("sales_col"), 0).alias("total_sales"))

print("Region Wise Sales:")
region_sales.show()

# ============================
# 2. CATEGORY SALES
# ============================
category_sales = df.groupBy("Category name") \
    .agg(round(sum("sales_col"), 0).alias("total_sales"))

print("Category Wise Sales:")
category_sales.show()

# ============================
# SAVE OUTPUT
# ============================
region_sales.write.mode("overwrite").csv("output/region_sales/")
category_sales.write.mode("overwrite").csv("output/category_sales/")

print("Week 3 Day 1 Completed")

spark.stop()