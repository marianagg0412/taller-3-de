import time
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import boto3

# Lightweight watcher that polls S3 for new parquet files under raw/ and
# invokes the pipeline processing function for each new file. This keeps
# processing automatic (no manual execution) while remaining simple to run
# on macOS or on a server as a service.

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("SECRET_KEY")
RAW_PREFIX = "raw/"
PROCESSED_MARKER_KEY = "metadata/processed_files.json"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
s3 = session.client("s3")

def list_raw_files():
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"]) 
    return files

def load_processed_files():
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=PROCESSED_MARKER_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []
    except Exception:
        return []

def update_processed_files(processed_files):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=PROCESSED_MARKER_KEY,
        Body=json.dumps(processed_files, indent=2).encode("utf-8"),
    )

def trigger_processing_for_new_files():
    # Import pipeline here to avoid starting Spark when this module is imported
    from pipeline import process_file

    all_files = list_raw_files()
    processed = load_processed_files()
    new_files = [f for f in all_files if f not in processed]

    if not new_files:
        print(f"{datetime.utcnow().isoformat()} - No new files found.")
        return

    print(f"{datetime.utcnow().isoformat()} - Found {len(new_files)} new file(s): {new_files}")

    for key in new_files:
        try:
            process_file(key)
            processed.append(key)
            update_processed_files(processed)
        except Exception as e:
            # Log the error and continue with other files
            print(f"Error processing {key}: {e}")

def main():
    print("Starting S3 watcher (polling) for new raw files...")
    try:
        while True:
            try:
                trigger_processing_for_new_files()
            except Exception as e:
                print(f"Watcher error: {e}")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Watcher stopped by user.")

if __name__ == "__main__":
    main()
