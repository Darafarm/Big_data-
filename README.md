🚀 Reactor Data Monitoring & Viscosity Prediction System
Author: James Daramola  
Department: Electrical & Computer Engineering  
Date: April 2026
📊 Model Performance
R²: 0.9978
RMSE: 23.02 cP
Mean Error: 0.76 cP
---
📌 Overview
This project implements a fully automated Big Data pipeline for monitoring a chemical reactor and predicting viscosity 10 minutes ahead.
Drop CSV files → Automatic processing → Live dashboard
End-to-end time: ~6 minutes
Manual effort: Zero
---
🏗️ Architecture
```
Plant Historian → Windows PC → AWS S3 → AWS EC2 → Dashboard
```
---
🗄️ Data Lake Layers
Bronze → Raw CSV
Silver → Cleaned data (Parquet)
Gold → ML features + predictions
---
⚙️ Pipeline Stages
Data Cleaning
Feature Engineering
Model Training
Prediction
---
🤖 Model
Random Forest Regressor
Train/Test: 80/20
Rows: 1.5M+
---
📈 Key Results
High accuracy (R² = 0.9978)
Faster than industrial tolerance
Low cost (~$0.05/hr)
---
🚀 How to Run
Start EC2
Update IP
Run watcher script
Access dashboard at port 8050
---
🛠️ Tech Stack
AWS (S3, EC2)
Hadoop + Spark
Python (ML + Dashboard)
---
📊 Output
Real-time predictions
Automated pipeline
Dashboard visualization
---
📌 Notes
Fully automated
Scalable
Production-ready
