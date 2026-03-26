##################
# data_uploader.py
##################

###################
# import statements
###################

import sys
import os
import boto3
from dotenv import load_dotenv
from pathlib import Path

##################
# global variables
##################

# define the folder and file paths for the csv data file
# create s3 client

folder_path = Path("../../data/raw")
file_name = Path("aeso_supply_and_demand.csv")
full_path = os.path.join(folder_path, file_name)
os.makedirs(folder_path, exist_ok=True)

# assign env variables
load_dotenv()
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION")
bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

##################
# module functions
##################

# verify the upload status by checking for bucket contents
def verify_upload():
    global bucket_name, region
    print("Verifying upload status...")

    try:
        s3 = boto3.client("s3", region_name=region)

        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix="raw_data/"
        )

        file_count = response["Contents"]
        if "Contents" not in response:
            print("No files found.")
        elif file_count:
            print(f"There are {len(file_count)} files.")

        total_size = 0
        for file in file_count:
            file_size = file["Size"] / (1024 * 1024)
            total_size += file_size

        print(f"Data size: {total_size: .2f} MB")
        print("Upload successfully verified.")

    except Exception as connection_error:
        print(f"Unable to verify upload status.\nError: {connection_error}")

# load the data into the s3 bucket
def data_load(s3):
    global folder_path, file_name, bucket_name
    csv_files = list(folder_path.glob("*.csv"))

    uploaded_count = 0
    for csv_file in csv_files:

        # create data lake structure
        s3_key = f"raw_data/{csv_file.name}"
        print(f"\nLoading data from {csv_file.name}...")

        try:
            s3.upload_file(csv_file, bucket_name, s3_key)
            print(f"Data upload successful.\nData uploaded to s3://{bucket_name}/{s3_key}\n")
            uploaded_count += 1

        except Exception as upload_error:
            print(f"Data upload failed.\nError: {upload_error}")

    if uploaded_count == len(csv_files):
        print(f"Successfully loaded {len(csv_files)} files.\n")
    else:
        print(f"Only {len(csv_files)} files were successfully loaded.")

# connect to the s3 bucket
def connect_aws():
    load_dotenv()

    if not bucket_name:
        print("Error: AWS_S3_BUCKET_NAME not found")

    print("Testing AWS connection...")
    print(f"Region: {region}")

    # attempt to connect to the s3 bucket
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )

        try:
            s3.head_bucket(Bucket=bucket_name)
            print("S3 bucket already exists.")
        except:
            print(f"Creating bucket {bucket_name}")
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
                print(f"Bucket {bucket_name} created.")

        # verify buckets
        print(f"Connection successful: Bucket {bucket_name} connected.")

        # upload the file to the s3 bucket
        data_load(s3)

    except Exception as e:
        print(f"Connection failed: {e}")

def main():
    # test the aws s3 connection
    connect_aws()

    # verify the file contents were uploaded successfully
    verify_upload()

if __name__ == "__main__":
    sys.exit(main())