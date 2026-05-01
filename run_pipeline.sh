#!/bin/bash
# =============================================================
# run_pipeline.sh
# Environment loader — called via SSH from watch_and_upload.ps1
#
# WHY THIS EXISTS:
# When SSH sends a command remotely, the server does not load
# the user's full environment (PATH, JAVA_HOME, etc.).
# Without this script, Spark and Python cannot be found.
#
# Usage:
#   bash /home/ubuntu/run_pipeline.sh filename.csv
#
# Called automatically by watch_and_upload.ps1 as:
#   ssh -i key.ppk ubuntu@IP '/home/ubuntu/run_pipeline.sh filename.csv'
# =============================================================

# Java environment
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Hadoop environment
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Spark environment
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Python/PySpark environment
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# $1 = CSV filename passed from watch_and_upload.ps1
# All output appended to pipeline.log
python3 /home/ubuntu/pipeline.py --input $1 >> /home/ubuntu/pipeline.log 2>&1
