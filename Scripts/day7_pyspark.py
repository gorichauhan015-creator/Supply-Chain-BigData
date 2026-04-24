from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Day7").getOrCreate()

df = spark.read.csv("Data/data.csv", header=True, inferSchema=True)

print("Full Data:")
df.show()

# 🔹 1. Category wise average price
print("Average Price by Category:")
df.groupBy("Category").agg(avg("Price").alias("AvgPrice")).show()

# 🔹 2. Highest price products
print("Products sorted by price (High to Low):")
df.orderBy(df.Price.desc()).show()

# 🔹 3. Electronics items only
print("Electronics Products:")
df.filter(df.Category == "Electronics").show()

spark.stop()