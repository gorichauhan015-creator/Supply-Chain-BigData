from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Week2_Day4").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("Data/Supply_chain_selected.csv")

print("Data Loaded")
df.show(5)

# =====================================
# Fix datatypes
# =====================================

df = df.withColumn("sales_col", col("Sales").cast("double"))
df = df.withColumn("shipping_real", col("Days for shipping (real)").cast("double"))

# =====================================
#  1. RUNNING TOTAL (Window Function)
# =====================================

windowSpec = Window.partitionBy("Order region").orderBy("Order id")

df = df.withColumn(
    "running_sales",
    sum("sales_col").over(windowSpec)
)

print("Running Sales by Region:")
df.select("Order id", "Order region", "sales_col", "running_sales").show(10)

# =====================================
#  2. MOVING AVERAGE (Window Function)
# =====================================

windowSpec2 = Window.partitionBy("Order region") \
                    .orderBy("Order id") \
                    .rowsBetween(-2, 0)

df = df.withColumn(
    "moving_avg_delivery",
    avg("shipping_real").over(windowSpec2)
)

print("Moving Average Delivery Time:")
df.select("Order id", "Order region", "shipping_real", "moving_avg_delivery").show(10)

spark.stop()