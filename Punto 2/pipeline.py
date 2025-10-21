import boto3
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, from_unixtime, year, month
from dotenv import load_dotenv

# -----------------------------
# Configuration
# -----------------------------
load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("SECRET_KEY")
RAW_PREFIX = "raw/"
CLEANED_PREFIX = "cleaned/"
CURATED_PREFIX = "curated/"
PROCESSED_MARKER_KEY = "metadata/processed_files.json"

# -----------------------------
# Init clients
# -----------------------------
s3_client = boto3.client("s3")

# Configure Spark with AWS S3 support
spark = (SparkSession.builder
    .appName("TaxiDataPipeline")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())

# -----------------------------
# Helper: get list of parquet files in RAW zone
# -----------------------------
def list_raw_files():
    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])
    return files

# -----------------------------
# Helper: load marker file of already processed files
# -----------------------------
def load_processed_files():
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=PROCESSED_MARKER_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        return []

# -----------------------------
# Helper: update marker file
# -----------------------------
def update_processed_files(processed_files):
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=PROCESSED_MARKER_KEY,
        Body=json.dumps(processed_files, indent=2).encode("utf-8"),
    )

# -----------------------------
# Cleaning + curated generation logic
# -----------------------------
def process_file(s3_key):
    print(f"🔹 Processing file: {s3_key}")
    df = spark.read.parquet(f"s3a://{BUCKET_NAME}/{s3_key}")

    print(f"📊 Initial row count: {df.count()}")
    df.printSchema()

    # 1. Normalize text
    df = df.withColumn("store_and_fwd_flag", upper(trim(col("store_and_fwd_flag"))))

    # 2. Filter invalid rows
    df_filtered = df.filter(
        (col("trip_distance") >= 0) &
        (col("total_amount") >= 0) &
        (col("passenger_count") > 0)
    )

    filtered_count = df_filtered.count()
    print(f"📊 After filtering: {filtered_count} rows")

    if filtered_count == 0:
        print("⚠️ WARNING: All rows were filtered out!")
        return

    df = df_filtered

    # 3. Add partition columns
    df = df.withColumn("year", year(col("tpep_pickup_datetime"))) \
           .withColumn("month", month(col("tpep_pickup_datetime")))

    # 4. Write cleaned parquet partitioned
    df.write.mode("append").partitionBy("year", "month").parquet(f"s3a://{BUCKET_NAME}/{CLEANED_PREFIX}")

    print(f"✅ Cleaned data written to {df.count()} rows.")

    # 5. Curated views
    revenue_by_payment = df.groupBy("payment_type").sum("total_amount")
    revenue_by_payment.write.mode("overwrite").parquet(f"s3a://{BUCKET_NAME}/{CURATED_PREFIX}revenue_by_payment_type.parquet")

    avg_tip = df.groupBy("DOLocationID").avg("tip_amount")
    avg_tip.write.mode("overwrite").parquet(f"s3a://{BUCKET_NAME}/{CURATED_PREFIX}avg_tip_by_zone.parquet")

    trips_by_month = df.groupBy("year", "month").count()
    trips_by_month.write.mode("overwrite").parquet(f"s3a://{BUCKET_NAME}/{CURATED_PREFIX}total_trips_by_month.parquet")

    print(f"✅ Finished {s3_key}")


# -----------------------------
# Main execution
# -----------------------------
if __name__ == "__main__":
    all_files = list_raw_files()
    processed_files = load_processed_files()

    new_files = [f for f in all_files if f not in processed_files]
    print(f"📂 Found {len(all_files)} files in raw/, {len(new_files)} new to process.")

    for key in new_files:
        process_file(key)
        processed_files.append(key)
        update_processed_files(processed_files)  # Save progress incrementally

    spark.stop()
    print("🏁 Pipeline completed successfully.")
