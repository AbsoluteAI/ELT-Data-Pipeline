#############
# data_extractor.py
#############

###################
# import statements
###################

import sys
import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv
from pyarrow import csv, parquet
import pandas as pd
import time
import glob
import data_uploader

##################
# global variables
##################

folder_path = Path("./data/raw")
file_name = Path("nasa_solar_flare_data")
full_path = os.path.join(folder_path, file_name)
# os.makedirs(folder_path, exist_ok=True)
seconds = 60
minutes = 1
step_interval = seconds * minutes
call_count = 0

load_dotenv()
nasa_api_key = os.getenv("NASA_API_KEY")
url = f"https://api.nasa.gov/DONKI/FLR?startDate=2016-01-01&endDate=2016-01-30&api_key={nasa_api_key}"

##################
# module functions
##################

def remove_files():
    pass

def file_incrementation(ext):
    global folder_path

    files = glob.glob(os.path.join(folder_path, "*" + ext))

    file_number = len(files) + 1
    # print(f"Found {file_number - 1} {ext} files")

    return file_number

def csv_to_parquet():
    global full_path

    csv_path = file_incrementation("csv") - 1

    table = csv.read_csv(f"{full_path} {csv_path}.csv")

    parquet_count = file_incrementation("parquet")

    parquet_path = f"{full_path} {parquet_count}.parquet"

    parquet.write_table(table, parquet_path)

    print(f"Data successfully saved to {parquet_path}")

    # df = pd.read_parquet(f"{full_path}.parquet")
    # print(df.head())

# connect to api dataset via gridstatus client
def create_csv_file(df):

    csv_count = file_incrementation("csv")

    new_path = f"{full_path} {csv_count}.csv"

    try:
        df.to_csv(new_path, index=False)
        print(f"Data successfully saved to {new_path}")
        return True
    except IOError as e:
        print(f"Error writing to csv file: {e}")
        return False

def create_json_file(data):
    global full_path, url

    json_count = file_incrementation("json")

    new_path = f"{full_path} {json_count}.json"

    try:
        with open(new_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {new_path}")
    except IOError as e:
        print(f"Error creating json file: {e}")

def run_poll():
    global url, call_count, step_interval

    while True:
        call_count +=1
        print(f"\nCall {call_count}:")

        try:
            response = requests.get(url)

            if requests.get(url).status_code == 200:
                data = response.json()
                print("Data successfully retrieved")

                create_json_file(data)

                df = pd.DataFrame(data)
                create_csv_file(df)

                csv_to_parquet()

                data_uploader.main()

            else:
                print(f"Status code: {response.status_code}")
        except Exception as e:
            print(f"Error retrieving data: {e}")

        try:
            print("\nNext call in:")

            duration_in_seconds = step_interval
            for remaining in range(duration_in_seconds, 0, -1):
                mins, secs = divmod(remaining, 60)
                timer_display = "{:02}:{:02}".format(mins, secs)

                sys.stdout.write("\r" + timer_display)
                sys.stdout.flush()
                time.sleep(1)
            sys.stdout.write("\n")

        except Exception as e:
            print(f"Timer error: {e}")

            return True

def main():
    run_poll()

if __name__ == "__main__":
    sys.exit(main())