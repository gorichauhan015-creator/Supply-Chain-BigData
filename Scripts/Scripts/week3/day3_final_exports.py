from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Week3_Day3").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("Supply_chain_selected.csv")

print("Data Loaded Successfully")
df.show(5, False)

# FULL data for dashboard
detailed_df = df

print("Detailed Full Data Preview")
detailed_df.show(10, False)

# Save output
detailed_df.write.mode("overwrite").csv("output/final_dashboard_data/")

spark.stop()