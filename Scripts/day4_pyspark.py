from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.master("local[*]").appName("Day4").getOrCreate()

# Sample data
data = [
    (1, "Laptop", 50000),
    (2, "Phone", 20000),
    (3, "Tablet", 30000),
    (4, "Mouse", 1000),
    (5, "Laptop", 55000),
    (6, "Phone", 25000)
]

columns = ["ID", "Product", "Price"]

df = spark.createDataFrame(data, columns)

print("Original Data:")
df.show()

# 🔹 withColumn (discount column add)
df = df.withColumn("DiscountPrice", col("Price") * 0.9)

print("With Discount:")
df.show()

# 🔹 groupBy + avg
print("Average Price by Product:")
df.groupBy("Product").agg(avg("Price").alias("AvgPrice")).show()

# 🔹 sorting
print("Sorted by Price (High to Low):")
df.orderBy(col("Price").desc()).show()

spark.stop()