import boto3
import os
import time

# ================= AWS CONFIG =================

s3 = boto3.client(
    's3',
    region_name='ap-south-1'
)

BUCKET_NAME = "declarative-ved"

# ================= WINDOWS FOLDERS =================

CSV_FOLDER = r"C:\Users\vedan\OneDrive\Documents\Regex\Dynamo_DB\all_Files\CSV FILES"

PARQUET_FOLDER = r"C:\Users\vedan\OneDrive\Documents\Regex\Dynamo_DB\all_Files\PARquet"

uploaded_files = set()

print("Checking folders every 30 seconds...")

while True:

    # ==========================================
    # CSV FILES
    # ==========================================

    csv_files = os.listdir(CSV_FOLDER)

    for file_name in csv_files:

        if file_name in uploaded_files:
            continue

        if file_name.endswith(".csv"):

            file_path = os.path.join(
                CSV_FOLDER,
                file_name
            )

            s3_key = f"csv-files/{file_name}"

            try:

                s3.upload_file(
                    file_path,
                    BUCKET_NAME,
                    s3_key
                )

                print(f"CSV Uploaded: {file_name}")

                uploaded_files.add(file_name)

            except Exception as e:

                print(f"CSV Error: {e}")

    # ==========================================
    # PARQUET FILES
    # ==========================================

    parquet_files = os.listdir(PARQUET_FOLDER)

    for file_name in parquet_files:

        if file_name in uploaded_files:
            continue

        if file_name.endswith(".parquet"):

            file_path = os.path.join(
                PARQUET_FOLDER,
                file_name
            )

            s3_key = f"parquet-files/{file_name}"

            try:

                s3.upload_file(
                    file_path,
                    BUCKET_NAME,
                    s3_key
                )

                print(f"Parquet Uploaded: {file_name}")

                uploaded_files.add(file_name)

            except Exception as e:

                print(f"Parquet Error: {e}")

    time.sleep(30)