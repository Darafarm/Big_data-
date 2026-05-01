"""
pipeline.py

 4-stage pipeline for Reactor Data Monitoring & Viscosity Prediction.

Stages:
    1. bronze_to_silver  — clean raw CSV data
    2. silver_to_gold    — engineer 13 ML features
    3. train_model       — train Random Forest, evaluate, save
    4. Predictions saved to S3 Gold automatically in stage 3

Usage:
    python3 pipeline.py --input files75_csv_combined.csv

Author: James Daramola
Date:   April 2026
"""

import argparse
import io
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# S3 Configuration 
BUCKET      = "reactor-project-emperor1"
SILVER_PATH = "s3a://reactor-project-emperor1/silver/reactor_data/"
GOLD_PATH   = "s3a://reactor-project-emperor1/gold/reactor_features/"
MODEL_PATH  = "s3a://reactor-project-emperor1/gold/viscosity_model/"
PRED_PATH   = "s3a://reactor-project-emperor1/gold/predictions/"


def get_spark():
    """Create and return a configured SparkSession with S3 access."""
    return SparkSession.builder \
        .appName("ReactorPipeline") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def standardise_columns(df_pandas):
    """
    Map any Historian column name to a standard short name.
    Also strips the BOM character (\\ufeff) added by UTF-16 files.

    This handles multiple export formats from different Historian versions.
    """
    col_map = {}
    for col in df_pandas.columns:
        # Strip invisible BOM character from UTF-16 files
        col_clean = col.replace("\ufeff", "").replace("\ufffe", "").strip()
        col_lower = col_clean.lower()

        if   "time" == col_lower.strip():                 col_map[col] = "timestamp"
        elif "batch time" in col_lower:                   col_map[col] = "batch_time_mins"
        elif "r1_visc_cp" in col_lower:                   col_map[col] = "viscosity_cp"
        elif "r1" in col_lower and "temp" in col_lower:   col_map[col] = "temperature_c"
        elif "r1_ph_r02" in col_lower:                    col_map[col] = "ph_level"
        elif "r1" in col_lower and "mass" in col_lower:   col_map[col] = "r1_mass"

    return df_pandas.rename(columns=col_map)


def bronze_to_silver(spark, input_file):
    """
    Stage 1 — Data Cleaning: Bronze → Silver.

    Reads raw CSV from S3 Bronze, fixes all quality issues,
    and saves clean Parquet to S3 Silver.

    Quality fixes applied:
        - UTF-16 encoding detection and BOM stripping
        - Column name standardisation
        - Comma removal from number formatting (64,734.01 → 64734.01)
        - String 'Null' replacement with proper Python None
        - Timestamp parsing with multiple format support
        - Numeric type casting
        - Zero sensor reading removal
        - Duplicate timestamp removal

    Args:
        spark:      Active SparkSession
        input_file: CSV filename in S3 Bronze bucket

    Returns:
        int: Number of clean rows saved to Silver
    """
    print(f"Stage 1: Cleaning data from {input_file}...")

    # Read raw bytes from S3 without saving to disk
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=f"bronze/{input_file}")
    raw = obj["Body"].read()

    # Detect encoding — try UTF-16 first (new Historian format), fall back to UTF-8
    try:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-16",
                         sep=None, engine="python")
        print("  Encoding: UTF-16")
    except Exception:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-8")
        print("  Encoding: UTF-8")

    # Standardise column names and strip BOM
    df = standardise_columns(df)
    print(f"  Columns after standardisation: {list(df.columns)}")

    # Remove commas from number formatting (Historian exports mass as 64,734.01)
    for col in df.columns:
        if col != "timestamp":
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)

    # Convert to Spark DataFrame for distributed processing
    df = spark.createDataFrame(df)

    # Replace string 'Null' values with proper null
    numeric_cols = ["batch_time_mins", "viscosity_cp", "temperature_c",
                    "ph_level", "r1_mass"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col,
                F.when(F.col(col).isin(["Null", "null", "NULL", "nan", "NaN"]),
                       None)
                 .otherwise(F.col(col)))

    # Cast timestamp — try multiple formats from different Historian versions
    for fmt in ["M/d/yyyy h:mm:ss.SSS a", "M/d/yyyy H:mm:ss",
                "yyyy-MM-dd HH:mm:ss", "M/d/yyyy h:mm a"]:
        df = df.withColumn("timestamp",
            F.coalesce(
                F.to_timestamp("timestamp", fmt),
                F.col("timestamp")
            ))

    # Cast all sensor columns to DoubleType
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Remove physically impossible zero readings
    # (sensor startup artefacts — batch_time=0 or ph=0 is impossible)
    df = df.filter(
        (F.col("batch_time_mins") > 0) &
        (F.col("ph_level") > 0)
    )

    # Remove duplicate timestamps
    # (Historian records multiple tags at same millisecond)
    before = df.count()
    df = df.dropDuplicates(["timestamp"])
    after = df.count()
    dupes = before - after
    print(f"  Removed {dupes:,} duplicate timestamps")

    # Add batch identifier from filename
    batch_id = input_file.replace(".csv", "").replace(".CSV", "")
    df = df.withColumn("batch_id", F.lit(batch_id))

    # Save as compressed Parquet to S3 Silver
    df.write.mode("overwrite") \
        .partitionBy("batch_id") \
        .parquet(SILVER_PATH)

    count = df.count()
    print(f"Stage 1 complete: {count:,} rows saved to Silver")
    return count


def silver_to_gold(spark):
    """
    Stage 2 — Feature Engineering: Silver → Gold.

    Creates 13 new ML columns from the clean Silver data.
    All Window functions are partitioned by batch_id to prevent
    cross-batch contamination — the critical innovation that
    improved R² from 0.41 to 0.9978.

    Features created:
        Rolling averages:  temp_rolling_avg_5, ph_rolling_avg_5, visc_rolling_avg_5
        Lag features:      temp_lag_1, viscosity_lag_1, viscosity_lag_5, viscosity_lag_10
        Rate of change:    temp_rate_change, ph_rate_change, visc_rate_change
        Batch context:     batch_progress_pct
        Prediction target: viscosity_target_10min (via lead(10))

    Args:
        spark: Active SparkSession

    Returns:
        int: Number of ML-ready rows saved to Gold
    """
    print("Stage 2: Engineering features...")
    df = spark.read.parquet(SILVER_PATH).orderBy("timestamp")

    #  CRITICAL: Partition by batch_id 
    # This prevents cross-batch contamination.
    # Without partitionBy: R² = 0.41
    # With partitionBy:    R² = 0.9978
    window_batch   = Window.partitionBy("batch_id").orderBy("timestamp")
    window_batch_5 = Window.partitionBy("batch_id").orderBy("timestamp") \
                           .rowsBetween(-5, 0)

    #  Rolling averages — remove sensor noise 
    df = df.withColumn("temp_rolling_avg_5",
        F.avg("temperature_c").over(window_batch_5))
    df = df.withColumn("ph_rolling_avg_5",
        F.avg("ph_level").over(window_batch_5))
    df = df.withColumn("visc_rolling_avg_5",
        F.avg("viscosity_cp").over(window_batch_5))

    #  Lag features — give model memory
    df = df.withColumn("temp_lag_1",
        F.lag("temperature_c", 1).over(window_batch))
    df = df.withColumn("viscosity_lag_1",
        F.lag("viscosity_cp", 1).over(window_batch))
    df = df.withColumn("viscosity_lag_5",
        F.lag("viscosity_cp", 5).over(window_batch))
    df = df.withColumn("viscosity_lag_10",
        F.lag("viscosity_cp", 10).over(window_batch))

    #  Rate of change — give model momentum 
    df = df.withColumn("temp_rate_change",
        F.col("temperature_c") - F.col("temp_lag_1"))
    df = df.withColumn("ph_rate_change",
        F.col("ph_level") - F.lag("ph_level", 1).over(window_batch))
    df = df.withColumn("visc_rate_change",
        F.col("viscosity_cp") - F.col("viscosity_lag_1"))

    #  Batch progress percentage — give model context 
    batch_stats = df.groupBy("batch_id").agg(
        F.min("batch_time_mins").alias("batch_start"),
        F.max("batch_time_mins").alias("batch_end"))
    df = df.join(batch_stats, on="batch_id", how="left")
    df = df.withColumn("batch_progress_pct",
        (F.col("batch_time_mins") - F.col("batch_start")) /
        (F.col("batch_end")       - F.col("batch_start")) * 100)

    #  Prediction target — viscosity 10 steps ahead
    # lead(10) looks FORWARD in time — opposite of lag()
    # This converts the problem to supervised regression
    df = df.withColumn("viscosity_target_10min",
        F.lead("viscosity_cp", 10).over(window_batch))

    # Drop rows missing target or key features
    feature_cols = ["temperature_c", "ph_level", "temp_rolling_avg_5",
                    "ph_rolling_avg_5", "visc_rolling_avg_5",
                    "temp_rate_change", "ph_rate_change", "visc_rate_change",
                    "viscosity_lag_1", "viscosity_lag_5", "viscosity_lag_10",
                    "batch_progress_pct", "batch_time_mins"]

    df = df.dropna(subset=feature_cols + ["viscosity_target_10min"])
    df = df.filter(F.col("viscosity_target_10min") > 0)

    # Save ML-ready data to S3 Gold
    df.write.mode("overwrite") \
        .partitionBy("batch_id") \
        .parquet(GOLD_PATH)

    count = df.count()
    print(f"Stage 2 complete: {count:,} rows saved to Gold")
    return count


def train_model(spark):
    """
    Stage 3 — ML Training: Gold → Trained Model + Predictions.

    Trains a Random Forest regressor on the engineered Gold features,
    evaluates on an 80/20 train/test split, and saves both the
    trained model and predictions to S3 Gold.

    Model configuration:
        Algorithm:  RandomForestRegressor
        Trees:      100
        Max depth:  15
        Seed:       42 (reproducible results)
        Split:      80% training / 20% testing

    Args:
        spark: Active SparkSession

    Returns:
        tuple: (rmse, r2) accuracy metrics
    """
    print("Stage 3: Training Random Forest model...")
    df = spark.read.parquet(GOLD_PATH)

    feature_cols = [
        "temperature_c", "ph_level",
        "temp_rolling_avg_5", "ph_rolling_avg_5", "visc_rolling_avg_5",
        "temp_rate_change", "ph_rate_change", "visc_rate_change",
        "viscosity_lag_1", "viscosity_lag_5", "viscosity_lag_10",
        "batch_progress_pct", "batch_time_mins"
    ]

    df = df.dropna(subset=feature_cols + ["viscosity_target_10min"])

    # Split 80% training / 20% testing
    # seed=42 ensures same split every run for reproducibility
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Training rows: {train_df.count():,}")
    print(f"  Test rows:     {test_df.count():,}")

    # VectorAssembler combines 13 feature columns into one vector column
    # Required by Spark MLlib before training
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    # Random Forest configuration
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="viscosity_target_10min",
        numTrees=100,    # 100 trees averaged = robust predictions
        maxDepth=15,     # deep enough to capture complex chemistry
        seed=42          # reproducible tree building
    )

    # Build and train the ML pipeline
    model = Pipeline(stages=[assembler, rf]).fit(train_df)

    # Evaluate on test set (never seen during training)
    preds = model.transform(test_df)
    evaluator = RegressionEvaluator(
        labelCol="viscosity_target_10min",
        predictionCol="prediction"
    )
    rmse = evaluator.setMetricName("rmse").evaluate(preds)
    r2   = evaluator.setMetricName("r2").evaluate(preds)

    print(f"  RMSE: {rmse:.4f} cP")
    print(f"  R2:   {r2:.4f}")

    # Print feature importance
    rf_model     = model.stages[-1]
    importances  = rf_model.featureImportances
    feat_imp     = sorted(zip(feature_cols, importances), key=lambda x: -x[1])
    print("\n  Feature Importance:")
    for feat, imp in feat_imp:
        print(f"    {feat:<25} {imp:.4f}  ({imp*100:.2f}%)")

    # Save trained model to S3
    model.write().overwrite().save(MODEL_PATH)
    print(f"\n  Model saved to: {MODEL_PATH}")

    # Save predictions table to S3 Gold
    preds.select(
        "timestamp", "temperature_c", "ph_level",
        "viscosity_target_10min", "prediction", "batch_id"
    ).write.mode("overwrite").parquet(PRED_PATH)
    print(f"  Predictions saved to: {PRED_PATH}")

    return rmse, r2


#  Entry point 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reactor viscosity prediction pipeline")
    parser.add_argument("--input", required=True,
        help="CSV filename in S3 Bronze bucket")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 60)
    print(f"REACTOR PIPELINE STARTING: {args.input}")
    print("=" * 60)

    bronze_to_silver(spark, args.input)
    silver_to_gold(spark)
    rmse, r2 = train_model(spark)

    print()
    print("=" * 60)
    print(f"PIPELINE COMPLETE")
    print(f"  RMSE : {rmse:.4f} cP")
    print(f"  R²   : {r2:.4f}")
    print("=" * 60)

    spark.stop()
