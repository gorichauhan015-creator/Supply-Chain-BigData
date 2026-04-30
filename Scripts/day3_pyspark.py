
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Day3").getOrCreate()

data = [
    (1, "Laptop", 50000),
    (2, "Phone", 20000),
    (3, "Tablet", 30000),
    (4, "Mouse", 1000)
]

columns = ["ID", "Product", "Price"]

df = spark.createDataFrame(data, columns)

print("Full Data:")
df.show()

print("Selected:")
df.select("Product", "Price").show()

print("Filtered:")
df.filter(df.Price > 20000).show()

print("Count:", df.count())

spark.stop()
