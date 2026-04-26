from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Week2_Day2").getOrCreate()

df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("Original Data:")
df.show(5)

# Remove nulls
df = df.dropna()

print("After removing NULL values:")
df.show(5)

# Remove duplicates
df = df.dropDuplicates()

print("After removing duplicates:")
df.show(5)

# Fix datatype
df = df.withColumn(
    "shipping_real",
    col("Days for shipping (real)").cast("double")
)

print("Datatype fixed:")
df.select("shipping_real").show(5)

print("Final Data:")
df.show(5)

print("Schema:")
df.printSchema()

