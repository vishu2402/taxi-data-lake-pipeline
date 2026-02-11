# 🚀 SparkFlow: Dynamic Incremental Data Pipeline

SparkFlow is a production-grade incremental data pipeline that ingests large CSV batches, handles schema evolution, and optimizes memory-intensive workloads using **Spark + Airflow + MinIO**.

## 📌 Project Overview
The goal was to build a resilient system that can automatically detect, process, and optimize data batches coming into a Landing Zone. The pipeline moves data from **MinIO (Raw)** to a **Silver Zone (Parquet)** using **PySpark** for transformations and **Apache Airflow** for orchestration.

![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-4B8BBE?style=for-the-badge)

---

## 🏗️ Architecture & Key Features

### 1. Dynamic File Discovery
Unlike traditional pipelines with hardcoded file lists, this system uses a **dynamic scanner** via Spark’s `binaryFile` format. It continuously polls the MinIO bucket and automatically detects new CSV batches without requiring code changes.

### 2. Intelligent Checkpointing (Idempotency)
To prevent duplicate processing, the pipeline implements a persistent state tracker (`processed_batches.txt`).  
Each successfully processed batch is logged, ensuring that after restarts or failures the pipeline resumes correctly with **zero data duplication**.

### 3. Schema Evolution
The pipeline gracefully handles schema changes across batches.  
From Batch 3 onward, newly introduced columns (e.g., `discount_percent`) are automatically incorporated using Spark’s `mergeSchema` capability.

![Architecture Diagram](images/incremental_pipeline_diagram.png)


---

## 🧨 The "Batch 6" Challenge: Solving OOM Errors
A major challenge occurred while processing **Batch 6 (1.8 GB)**, which initially caused the Spark Driver to crash with:

`java.lang.OutOfMemoryError: Java heap space`

### Resolution Strategy
- **Optimized Partitioning:** Used `.repartition(40)` to split the 1.8GB file into ~45MB chunks
- **Memory Tuning:** Increased Driver & Executor memory to 4GB and enabled 2GB off-heap storage
- **Shuffle Optimization:** Set `spark.sql.shuffle.partitions = 40` to avoid skewed tasks
- **Storage Efficiency:** Switched to **Parquet with Snappy compression** for faster I/O and reduced storage size

## 🛠️ Tech Stack

| Component | Technology |
|--------|------------|
| **Storage** | MinIO (S3 compatible) |
| **Processing** | Apache Spark (PySpark) |
| **Orchestration** | Apache Airflow |
| **Format** | Parquet (Snappy) |
| **OS** | Linux / Ubuntu |

---

## 🚀 How to Run

### 1. Environment Setup
Create a virtual environment and install dependencies:
```bash
pip install -r requirements.txt
```

---

### 2. Infrastructure Setup

#### MinIO
Start your local MinIO server and create the required buckets:
- `project2` → Landing Zone (Raw CSV)
- `silver-clean` → Silver Zone (Processed Parquet)

#### Airflow
Install and configure Apache Airflow.  
Place the DAG file in the Airflow DAGs directory:
```bash
~/airflow/dags/taxi_final_pipeline.py
```

---
### 3. Configuration
Update Spark–MinIO connection details in the pipeline script:
- `access_key`
- `secret_key`
- MinIO endpoint

---

### 4. Execution
Dataset used - https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

1. Download the dataset and extract the zip folder
2. Upload the csv files in minio bucket `project2`

Start Airflow Webserver and Scheduler:
```bash
airflow webserver --port 8080
airflow scheduler
```

The Airflow DAG will automatically:
- Detect the new file
- Trigger the Spark job
- Apply transformations and optimizations
- Write optimized Parquet data to the Silver Zone

