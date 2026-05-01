from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day6").getOrCreate()

df = spark.read.csv("Data/data.csv", header=True, inferSchema=True)

# 1. Null values hatao
print("After removing null values:")
df = df.dropna()
df.show()

# 2. Price > 20000 filter
print("Price greater than 20000:")
df.filter(df.Price > 20000).show()

# 3. Sirf 2 column select karo
print("Selected columns:")
df.select("Product", "Price").show()

# 4. Count total rows
print("Total rows:", df.count())

spark.stop()