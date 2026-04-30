from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder.appName("Week2_Day6").getOrCreate()

# =====================================
# 1. LOAD DATA
# =====================================

df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("Data Loaded")
df.show(5)

# =====================================
# 2. FIX DATATYPE
# =====================================

df = df.withColumn("sales_col", col("Sales").cast("double"))

# =====================================
# 3. REGION-WISE SALES
# =====================================

region_sales = df.groupBy("Order region") \
    .agg(sum("sales_col").alias("total_sales"))

print("Region-wise Sales:")
region_sales.show()

# =====================================
# 4. CATEGORY-WISE SALES
# =====================================

category_sales = df.groupBy("Category name") \
    .agg(sum("sales_col").alias("total_sales"))

print("Category-wise Sales:")
category_sales.show()

# =====================================
# 5. TOP PRODUCTS
# =====================================

top_products = df.groupBy("Product name") \
    .agg(sum("sales_col").alias("total_sales")) \
    .orderBy(desc("total_sales"))

print("Top Selling Products:")
top_products.show(10)

# =====================================
# 6. SAVE OUTPUTS
# =====================================

region_sales.write.mode("overwrite").csv("Output/region_sales")
category_sales.write.mode("overwrite").csv("Output/category_sales")
top_products.write.mode("overwrite").csv("Output/top_products")

print(" All outputs saved successfully")

spark.stop()