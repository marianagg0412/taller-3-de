import boto3
import os
import re
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')

bucket_name = os.getenv("BUCKET_NAME")
local_folder = "data/"
s3_prefix = "raw/"

for file_name in os.listdir(local_folder):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(local_folder, file_name)

        # Extract year and month from filename (e.g., yellow_tripdata_2025-01.parquet)
        match = re.search(r'(\d{4})-(\d{2})', file_name)
        if match:
            year = match.group(1)
            month = match.group(2)
            s3_key = f"{s3_prefix}{year}/{month}/{file_name}"
        else:
            # Fallback if pattern not found
            s3_key = f"{s3_prefix}{file_name}"

        # Check if file already exists in S3
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"Skipping {file_name} - already exists at s3://{bucket_name}/{s3_key}")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # File doesn't exist, upload it
                print(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
                s3.upload_file(file_path, bucket_name, s3_key)
            else:
                # Some other error occurred
                print(f"Error checking {file_name}: {e}")
                raise
