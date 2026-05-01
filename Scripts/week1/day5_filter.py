from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day5").getOrCreate()

df = spark.read.csv("Data/data.csv", header=True, inferSchema=True)

df.show()


print("First 3 rows:")
df.show(3)

print("Schema:")
df.printSchema()

print("Electronics items:")
df.filter(df.Category == "Electronics").show()

spark.stop()