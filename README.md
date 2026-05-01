Reactor Data Monitoring & Viscosity Prediction System
Author: James Daramola  
Department: Electrical and Computer Engineering  
Date: April 2026  
Model Accuracy: R² = 0.9978 | RMSE = 23.02 cP | Mean Error = 0.76 cP
---
What This System Does
This is a fully automated Big Data pipeline that monitors a chemical reactor and predicts viscosity 10 minutes ahead. The operator drops a folder of CSV files into a monitored folder on a Windows PC. Within 6 minutes, the system has cleaned the data, engineered 13 machine learning features, trained a Random Forest model, and updated a live web dashboard showing predictions, all automatically with zero manual steps.
---


```powershell
# 1. Start  and check EC2
aws ec2 start-instances --instance-ids i-084fd00b0356d1b12

# 2. Get new IP (changes on every restart)
aws ec2 describe-instances --query "Reservations\\\[\\\*].Instances\\\[\\\*].\\\[InstanceId,State.Name,PublicIpAddress]" --output table

# 3. Update watcher script with new IP
(Get-Content "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\\\\\_and\\\\\\\_upload.ps1") -replace 'OLD-IP', 'NEW-IP' | Set-Content "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\\\\\_and\\\\\\\_upload.ps1"


(Get-Content "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\_and\\\_upload.ps1") -replace '54.144.163.39', '54.166.69.149' | Set-Content "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\_and\\\_upload.ps1"

4. Verify the update
Select-String -Path "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\_and\\\_upload.ps1" -Pattern "ec2IP"


# 4. Start the watcher
powershell -ExecutionPolicy Bypass -File "C:\\\\Users\\\\daram\\\\ReactorProject\\\\scripts\\\\watch\\\_and\\\_upload.ps1"

```

Get-ChildItem "C:\Users\daram\ReactorProject\incoming_csv\"
Remove-Item "C:\Users\daram\ReactorProject\incoming_csv\*" -Recurse -Force -Exclude "*.xlsx"


Connect to EC2 via PuTTY
```
Host Name:  ubuntu@\\\\\\\\\\\\\\\[CURRENT EC2 IP]
Port:       22
Key file:   C:\\\\\\\\\\\\\\\\Users\\\\\\\\\\\\\\\\daram\\\\\\\\\\\\\\\\ReactorProject\\\\\\\\\\\\\\\\reactor-key-final.ppk
```
Start Hadoop and dashboard on EC2
```bash
start-dfs.sh \\\&\\\& start-yarn.sh
jps  # must show 5 services

nohup python3 \\\~/dashboard.py > \\\~/dashboard.log 2>\\\&1 \\\&

tail -5 \\\~/dashboard.log// checking if started 



\\\\# Access at: http://\\\\\\\[EC2-IP]:8050

http://54.166.69.149:8050

tail -f \\\~/pipeline.log // Check Progress on EC2 via PuTTY
```
Process new data
Drop the data folder into:
```
C:\\\\\\\\\\\\\\\\Users\\\\\\\\\\\\\\\\daram\\\\\\\\\\\\\\\\ReactorProject\\\\\\\\\\\\\\\\incoming\\\\\\\\\\\\\\\_csv\\\\\\\\\\\\\\\\
```
Everything else happens automatically.

at the end of the day

```powershell
aws ec2 stop-instances --instance-ids i-084fd00b0356d1b12
```
---
System Architecture
```
Plant Historian → Windows PC → AWS S3 Bronze → AWS EC2 → Dashboard
                  (watcher)    (raw archive)   (Spark)    (:8050)
```
Data Lake Layers (AWS S3 — reactor-project-emperor1)
Layer	Path	Contents	Format
Bronze	s3://reactor-project-emperor1/bronze/	Raw CSV files — never modified	CSV
Silver	s3://reactor-project-emperor1/silver/	Cleaned data — nulls fixed, deduped	Parquet
Gold	s3://reactor-project-emperor1/gold/	ML features + model + predictions	Parquet
---
Scripts
Windows PC
Script	Location	Purpose
watch_and_upload.ps1	scripts/	Monitors incoming_csv\ — merges CSVs, uploads to S3, triggers pipeline via SSH
start_reactor.ps1	scripts/	Morning auto-startup — starts EC2, updates IP, prints connection info
EC2 Server (/home/ubuntu/)
Script	Purpose
pipeline.py	Master pipeline — runs all 4 stages automatically
run_pipeline.sh	Environment loader — sets JAVA_HOME, SPARK_HOME, PYTHONPATH before pipeline
dashboard.py	Plotly Dash web dashboard — reads S3 Gold, refreshes every 60 seconds
start_hadoop.sh	Auto-starts Hadoop on EC2 boot via crontab @reboot
---
Pipeline Stages
Stage 1: Clean Data (Bronze → Silver, ~15 seconds)
Reads raw CSV from S3 Bronze using boto3
Detects UTF-16 encoding and strips BOM character (\ufeff)
Replaces string 'Null' values with proper Python None
Removes 51,711 duplicate timestamps
Removes zero sensor readings (startup artefacts)
Renames long Historian column names to short clean names
Saves as compressed Parquet to S3 Silver
Stage 2  Engineer Features (Silver → Gold, ~15 seconds)
Creates 13 new columns using PySpark Window functions, all partitioned by batch_id to prevent cross-batch contamination:
Feature	Type	Purpose
temp_rolling_avg_5	Rolling average	Smooth sensor noise
ph_rolling_avg_5	Rolling average	Smooth sensor noise
visc_rolling_avg_5	Rolling average	Smooth sensor noise
viscosity_lag_1	Memory	Where was viscosity 1 step ago?
viscosity_lag_5	Memory	Where was viscosity 5 steps ago?
viscosity_lag_10	Memory	Where was viscosity 10 steps ago?
temp_lag_1	Memory	Previous temperature reading
temp_rate_change	Momentum	How fast is temperature changing?
ph_rate_change	Momentum	How fast is pH changing?
visc_rate_change	Momentum	How fast is viscosity changing?
batch_progress_pct	Context	What % of the batch is complete?
batch_time_mins	Context	Total minutes elapsed in batch
viscosity_target_10min	Label	Actual viscosity 10 steps ahead (lead(10))
Critical: .partitionBy('batch_id') on every Window function.  
Without it: R² = 0.41. With it: R² = 0.9978.
Stage 3 — Train Model (Gold → Model, ~25 seconds)
```python
RandomForestRegressor(numTrees=100, maxDepth=15, seed=42)
train/test split: 80% / 20%   (seed=42 for reproducibility)
training rows:    1,264,251
test rows:        315,745
R²:               0.9978
RMSE:             23.02 cP
mean error:       0.76 cP
```
Stage 4 — Save Predictions (~5 seconds)
Saves a table of actual vs predicted viscosity for every test row to S3 Gold. This table is what the dashboard reads.
---
Key Results
Metric	Value	Meaning
R²	0.9978	Model explains 99.78% of all viscosity variation
RMSE	23.02 cP	Average prediction error across 315,745 test rows
Mean absolute error	0.76 cP	Typical error in stable batch conditions
Plant tolerance	±50 cP	Our model is 65× more precise than required
Pipeline speed	~60 sec	EC2 processing time per run
End-to-end time	~6 min	Folder drop to updated dashboard
AWS cost	~$0.05/hr	Stop EC2 when not in use to save credits
---
Feature Importance (from trained model)
Rank	Feature	Importance
1	ph_level	23.47%
2	ph_rolling_avg_5	19.63%
3	temperature_c	19.11%
4	batch_time_mins	14.56%
5	temp_rate_change	9.76%
6	ph_rate_change	7.32%
7	batch_progress_pct	5.12%
8	viscosity_lag_1	1.03%
pH is the dominant driver — consistent with alkaline-catalysed condensation polymerisation theory.
---
Troubleshooting
Problem	Fix
SSH connection timed out	EC2 IP changed. Get new IP and update watcher script.
Server refused our key	Add public key via AWS Console → EC2 Instance Connect
NameNode missing from jps	Run: hdfs namenode -format -force then restart Hadoop
Dashboard blank	pkill -f dashboard.py then restart it
Pipeline ModuleNotFoundError	Use run_pipeline.sh which sets all environment variables
Port 8050 not accessible	Open port in security group: aws ec2 authorize-security-group-ingress --port 8050
---
Infrastructure Details
Component	Value
AWS Account	175610343122
S3 Bucket	reactor-project-emperor1
EC2 Instance ID	i-084fd00b0356d1b12
EC2 Type	m7i-flex.large
OS	Ubuntu 22.04
Java	OpenJDK 11.0.30
Hadoop	3.3.6
Spark	3.5.1
Hive	3.1.3
Python	3.10
---
Viewing Stored Data
```bash
# List all files across all S3 layers
aws s3 ls s3://reactor-project-emperor1/ --recursive --human-readable

# View cleaned Silver data (run on EC2)
---
python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewSilver") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/silver/reactor_data/")
print(f"Total clean rows: {df.count():,}")
print(f"Columns: {df.columns}")
df.show(20, truncate=False)
spark.stop()
EOF

# View ML-ready Gold data (run on EC2)
python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewGold") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/gold/reactor_features/")
print(f"Total ML-ready rows: {df.count():,}")
print(f"Columns: {df.columns}")
df.show(20, truncate=False)
spark.stop()
EOF

# View prediction results (run on EC2)
python3 << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ViewPredictions") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://reactor-project-emperor1/gold/predictions/")
df = df.withColumn("error", F.abs(F.col("prediction") - F.col("viscosity_target_10min")))
print(f"Total predictions: {df.count():,}")
df.select("timestamp","temperature_c","ph_level",
          F.round("viscosity_target_10min",2).alias("actual"),
          F.round("prediction",2).alias("predicted"),
          F.round("error",2).alias("error_cP")
).orderBy("timestamp").show(20, truncate=False)
spark.stop()
EOF

# Check pipeline results
tail -30 ~/pipeline.log

Reactor Data Monitoring & Viscosity Prediction System - James Daramola — April 2026
