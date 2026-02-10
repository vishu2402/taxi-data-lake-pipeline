import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = (SparkSession.builder 
    .appName("IncrementalLake")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate())

raw_path = "s3a://project2/batch_1.csv"
df_raw = spark.read.option("header", "true").csv(raw_path)

df_clean = df_raw.withColumn("processing_date", current_date())

silver_path = "s3a://silver-clean/orders_data/"

df_clean.write \
    .format("parquet") \
    .partitionBy("processing_date") \
    .mode("append") \
    .save(silver_path)

print("Batch 1 successfully moved to Silver Layer with Partitioning!")