"""
view_silver.py
==============
Inspect cleaned Silver data stored in S3.

Usage (on EC2):
    python3 view_silver.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewSilver") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/silver/reactor_data/")

print("=== SILVER DATA SUMMARY ===")
print(f"Total clean rows: {df.count():,}")
print(f"Columns: {df.columns}")
print()
print("=== SAMPLE ROWS ===")
df.show(20, truncate=False)
print()
print("=== DATE RANGE ===")
df.agg(F.min("timestamp"), F.max("timestamp")).show()
print()
print("=== VISCOSITY STATS ===")
df.agg(
    F.count("viscosity_cp").alias("count"),
    F.min("viscosity_cp").alias("min_cP"),
    F.max("viscosity_cp").alias("max_cP"),
    F.avg("viscosity_cp").alias("avg_cP")
).show()
print()
print("=== ROWS PER BATCH ===")
df.groupBy("batch_id").count().orderBy("batch_id").show(30)

spark.stop()
