from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import boto3
import io
import time

# ================= GOOGLE DRIVE CONFIG =================

SERVICE_ACCOUNT_FILE = "service-account.json"

SCOPES = ['https://www.googleapis.com/auth/drive']

# GOOGLE DRIVE FOLDER ID
FOLDER_ID = "1c5HUqWkI9GBgWombO4ZIGEn3JoqlXfoT"

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

drive_service = build(
    'drive',
    'v3',
    credentials=credentials
)

# ================= AWS CONFIG =================

s3 = boto3.client(
    's3',
    region_name='ap-south-1'
)

dynamodb = boto3.resource(
    'dynamodb',
    region_name='ap-south-1'
)

# ================= DYNAMODB TABLE =================

table = dynamodb.Table("ved-ec-2")

# ================= S3 BUCKET =================

BUCKET_NAME = "declarative-ved"

# ================= SAVE STATUS TO DYNAMODB =================

def save_file_status(file_id, file_name, s3_path):

    table.put_item(
        Item={
            "Status_Key": file_id,
            "file_name": file_name,
            "s3_path": s3_path,
            "status": "processed"
        }
    )

# ================= PROCESS FILES =================

def process_files():

    query = f"'{FOLDER_ID}' in parents and trashed=false"

    results = drive_service.files().list(
        q=query,
        pageSize=100,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])

    print(f"\nTotal files found: {len(files)}")

    if len(files) == 0:
        print("No files found")
        return

    for file in files:

        file_id = file['id']
        file_name = file['name']

        print(f"\nProcessing File: {file_name}")

        # ================= ONLY JSON FILES =================

        if file_name.lower().endswith(".json"):

            s3_key = f"json-files/{file_name}"

        else:

            print(f"Skipping File: {file_name}")

            continue

        # ================= DOWNLOAD FILE FROM GOOGLE DRIVE =================

        request = drive_service.files().get_media(
            fileId=file_id
        )

        file_stream = io.BytesIO()

        downloader = MediaIoBaseDownload(
            file_stream,
            request
        )

        done = False

        while done is False:

            status, done = downloader.next_chunk()

        file_stream.seek(0)

        # ================= UPLOAD JSON FILE TO S3 =================

        s3.upload_fileobj(
            file_stream,
            BUCKET_NAME,
            s3_key
        )

        print(f"Uploaded to S3: {s3_key}")

        # ================= SAVE TO DYNAMODB =================

        save_file_status(
            file_id,
            file_name,
            s3_key
        )

        print("Inserted into DynamoDB")

# ================= AUTOMATIC CHECK EVERY 30 SECONDS =================

while True:

    print("\nChecking Google Drive for new JSON files...\n")

    process_files()

    print("\nWaiting 30 seconds for next check...\n")

    time.sleep(30)