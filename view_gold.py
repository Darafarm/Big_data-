"""
view_gold.py
============
Inspect ML-ready Gold data stored in S3.

Usage (on EC2):
    python3 view_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewGold") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/gold/reactor_features/")

print("=== GOLD DATA SUMMARY ===")
print(f"Total ML-ready rows: {df.count():,}")
print(f"Total columns: {len(df.columns)}")
print(f"Columns: {df.columns}")
print()
print("=== SAMPLE ROWS — ALL 13 FEATURES ===")
df.select(
    F.col("timestamp"),
    F.round("temperature_c",     2).alias("temp_C"),
    F.round("ph_level",          3).alias("pH"),
    F.round("viscosity_cp",      2).alias("visc_cP"),
    F.round("temp_rolling_avg_5",2).alias("temp_avg5"),
    F.round("ph_rolling_avg_5",  3).alias("ph_avg5"),
    F.round("viscosity_lag_1",   2).alias("visc_lag1"),
    F.round("viscosity_lag_5",   2).alias("visc_lag5"),
    F.round("viscosity_lag_10",  2).alias("visc_lag10"),
    F.round("temp_rate_change",  3).alias("temp_rate"),
    F.round("ph_rate_change",    3).alias("ph_rate"),
    F.round("batch_progress_pct",1).alias("batch_%"),
    F.round("viscosity_target_10min", 2).alias("target_10min")
).orderBy("timestamp").show(30, truncate=False)

print()
print("=== FEATURE STATISTICS ===")
df.agg(
    F.min("viscosity_cp").alias("min_visc"),
    F.max("viscosity_cp").alias("max_visc"),
    F.avg("viscosity_cp").alias("avg_visc"),
    F.avg("batch_progress_pct").alias("avg_batch_%"),
    F.avg("temp_rate_change").alias("avg_temp_rate")
).show()

print()
print("=== ROWS PER BATCH ===")
df.groupBy("batch_id").count().orderBy("batch_id").show(30)

spark.stop()
