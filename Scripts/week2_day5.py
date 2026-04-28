from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder.appName("Week2_Day5").getOrCreate()

df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

df = df.withColumn("sales_col", col("Sales").cast("double"))

# Cache data
df.cache()

# Aggregation
result = df.groupBy("Order region") \
           .agg(sum("sales_col").alias("total_sales"))

print("Explain Plan:")
result.explain()

print("Final Result:")
result.show()

spark.stop()