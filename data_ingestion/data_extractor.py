#############
# data_extractor.py
#############

###################
# import statements
###################

import sys
from gridstatusio import GridStatusClient
import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv
from pyarrow import csv, parquet
import pandas as pd
import polling2
import logging

##################
# global variables
##################

folder_path = Path("../../data/raw")
file_name = Path("aeso_supply_and_demand")
full_path = os.path.join(folder_path, file_name)
os.makedirs(folder_path, exist_ok=True)

load_dotenv()
gridstatus_api_key = os.getenv("GRIDSTATUS_API_KEY")
url = f"https://api.gridstatus.io/v1/datasets/aeso_supply_and_demand/query?start_time=2026-03-24&end_time=2026-03-27&timezone=market&api_key={gridstatus_api_key}"

##################
# module functions
##################

def csv_to_parquet():
    global full_path

    table = csv.read_csv(f"{full_path}.csv")

    parquet.write_table(table, f"{full_path}.parquet")

    print(f"Data successfully saved to {full_path}.parquet")

    df = pd.read_parquet(f"{full_path}.parquet")
    print(df.head())

# connect to api dataset via gridstatus client
def create_csv_file():
    global gridstatus_api_key

    # try connecting to gridstatus client with api key
    try:
        client = GridStatusClient(gridstatus_api_key)

        # Fetch data as pandas DataFrame
        df = client.get_dataset(
          dataset="aeso_supply_and_demand",
          start="2026-03-22",
          end="2026-03-25",
          timezone="market",
        )
        print("Saving contents to csv file...")
        try:
            df.to_csv(f"{full_path}.csv", index=False)
            print(f"Data successfully saved to {full_path}.csv")
        except IOError as e:
            print(f"Error writing to csv file: {e}")

    # handle exception with error description output
    except Exception as e:
        print(f"Data extraction failed.\nError: {e}")

def create_json_file(data):
    global full_path, url

    print("Saving contents to json file...")

    try:
        with open(f"{full_path}_{count}.json", 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {full_path}_{count}.json")
    except IOError as e:
        print(f"Error creating json file: {e}")

def retrieve_api_data():
    global url

    print("Connecting to data source...")
    try:
        response = requests.get(url, stream=True, timeout=10)

        if response.status_code == 200:
            print("Connection successful\n")
            data = response.json()
            return data
        else:
            print(f"Request failed: {response.status_code}, {response.reason}")

    except requests.exceptions.HTTPError as e:
        print(f"Connection failed: {e}")
        return False
    except requests.exceptions.Timeout as e:
        print(f"Connection timed out: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection failed: {e}")
    except Exception as e:
        print(f"Failed to retrieve data: {e}")

def run_poll():
    global url

    print(f"Initializing polling...")

    while True:
        try:
            new_data = polling2.poll(
                target=retrieve_api_data,
                step=60,
                poll_forever=True,
                ignore_exceptions = (requests.exceptions.ConnectionError, ValueError),
                log_error=logging.DEBUG
            )

            create_json_file(new_data)
            create_csv_file()
            csv_to_parquet()

        except polling2.TimeoutException:
            print("Polling timed out. The expected status was not reached within the time limit.")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during API requests: {e}")

def main():
    run_poll()

if __name__ == "__main__":
    sys.exit(main())