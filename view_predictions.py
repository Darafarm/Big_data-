"""
view_predictions.py
===================
Inspect prediction results stored in S3 Gold.

Usage (on EC2):
    python3 view_predictions.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewPredictions") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/gold/predictions/")
df = df.withColumn("error",
    F.abs(F.col("prediction") - F.col("viscosity_target_10min")))

print("=== PREDICTIONS SUMMARY ===")
print(f"Total predictions: {df.count():,}")
print()
print("=== SAMPLE PREDICTIONS ===")
df.select(
    "timestamp",
    F.round("temperature_c",          2).alias("temp_C"),
    F.round("ph_level",               3).alias("pH"),
    F.round("viscosity_target_10min", 2).alias("actual_cP"),
    F.round("prediction",             2).alias("predicted_cP"),
    F.round("error",                  2).alias("error_cP")
).orderBy("timestamp").show(30, truncate=False)

print()
print("=== ACCURACY SUMMARY ===")
df.agg(
    F.round(F.avg("error"),       2).alias("mean_abs_error_cP"),
    F.round(F.min("prediction"),  2).alias("min_predicted_cP"),
    F.round(F.max("prediction"),  2).alias("max_predicted_cP"),
    F.round(F.avg("prediction"),  2).alias("avg_predicted_cP"),
    F.round(F.max("error"),       2).alias("max_error_cP")
).show()

spark.stop()
