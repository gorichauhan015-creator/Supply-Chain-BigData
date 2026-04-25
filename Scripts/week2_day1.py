from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Start Spark
spark = SparkSession.builder.appName("Week2_Day1").getOrCreate()

# Load dataset
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("✅ Data Loaded Successfully\n")

# Show sample data
df.show(5)

# Show column names
print("Columns are:")
print(df.columns)

# Convert shipping column to numeric
df = df.withColumn(
    "shipping_real",
    col("Days for shipping (real)").cast("double")
)

# ================================
# 🔥 GROUP BY (MAIN OUTPUT)
# ================================

print("\nAverage Delivery Time by Region:")

df.groupBy("Order region") \
  .agg(avg("shipping_real").alias("avg_delay")) \
  .show()

# Stop Spark
spark.stop()
