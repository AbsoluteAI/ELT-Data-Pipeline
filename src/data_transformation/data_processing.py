####################
# data_processing.py
####################

###################
# import statements
###################

import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

##################
# global variables
##################


##################
# module functions
##################

def extract_data():
    pass

def transform_data():
    pass

def load_data():
    pass

def process_data():

    load_dotenv()
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    region = os.getenv("AWS_DEFAULT_REGION")

    if not bucket_name:
        print("S3 bucket does not exist")
        return False
    print(f"Connecting to S3 bucket {bucket_name}")

    try:
        # extract s3 data
        dataset = extact_data(s3, bucket_name)

        # clean and transform the data
        processed_data = transform_data(dataset)

        # export data for external use
        upload_data = load_data(s3, bucket_name, processed_data)

        if upload_data:
            print("Data upload sucessful")
            return True
        else:
            print("Data upload failed")
            return False

    except Exception as e:
        print(f"Error processing data: {e}")
        return False


if __name__ == "__main__":
    process_data()