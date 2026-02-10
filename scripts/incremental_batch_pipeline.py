import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit

spark = (
    SparkSession.builder
    .appName("DynamicIncrementalPipeline")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    
    .config("spark.hadoop.fs.s3a.connection.timeout", 60000)
    .config("spark.sql.parquet.compression.codec", "uncompressed")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

checkpoint_file = "/home/vishal/de_project/scripts/processed_batches.txt"
raw_bucket_path = "s3a://project2/"
silver_path = "s3a://silver-clean/orders_parquet_table/"

def get_processed_files():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return set(f.read().splitlines())
    return set()

processed_files = get_processed_files()

print("Scanning MinIO for new batches...")
try:
    raw_files_df = spark.read.format("binaryFile").load(raw_bucket_path)
    all_batches = [
        os.path.basename(row.path) 
        for row in raw_files_df.select("path").collect() 
        if row.path.endswith(".csv")
    ]
    all_batches.sort()
    print(f"Found total {len(all_batches)} files in bucket.")
except Exception as e:
    print("Could not scan MinIO bucket.")
    raise e

for batch in all_batches:
    if batch in processed_files:
        print(f"⏭Skipping: {batch}")
        continue

    print(f"\nProcessing New Batch: {batch}")

    try:
        df_raw = spark.read.option("header", "true").csv(f"{raw_bucket_path}{batch}")

        for c in df_raw.columns:
            df_raw = df_raw.withColumnRenamed(c, c.lower())

        try:
            batch_num = int(re.search(r'\d+', batch).group())
        except:
            batch_num = 0

        if batch_num >= 3:
            df_clean = df_raw.withColumn("processing_date", current_date()).withColumn("discount_percent", lit(10))
        else:
            df_clean = df_raw.withColumn("processing_date", current_date())

        (
            df_clean
            .repartition(40)
            .write
            .mode("append")
            .format("parquet")
            .option("mergeSchema", "true")
            .partitionBy("processing_date")
            .save(silver_path)
        )

        with open(checkpoint_file, "a") as f:
            f.write(batch + "\n")
        
        print(f"Successfully processed and checkpointed: {batch}")

    except Exception as e:
        print(f"FATAL ERROR in {batch}: {str(e)}")
        raise e 

print("\nSilver Zone Data Preview")
try:
    spark.read.option("mergeSchema", "true").parquet(silver_path).show(5)
except:
    print("No data in Silver zone yet.")