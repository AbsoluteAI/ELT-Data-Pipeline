# data_extractor_oltp.py

# import statements
from dotenv import load_dotenv
import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# global variables
load_dotenv()
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_host = os.getenv("MYSQL_HOST")
mysql_database = os.getenv("MYSQL_DATABASE")
folder_path = Path("./data/raw")

def fetch_data():
    global folder_path, mysql_username, mysql_password, mysql_host, mysql_database
    try:
        csv_files = list(folder_path.glob("*.csv"))

        print(f"\nSaving csv data to new table:")

        try:
            count = 0
            for file in csv_files:
                file_name = file.name
                print(file_name.removesuffix(".csv"))
                df = pd.read_csv(file)
                engine = create_engine(f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_database}")
                df.to_sql(file_name.removesuffix(".csv"), con=engine, if_exists='replace', index=False, chunksize=10000)
                count += 1

            print(f"\nSuccessfully wrote {count} files to database")

        except Exception as e:
            print(f"Unable to write data to database: {e}")

    except Exception as e:
        print(f"Could not find any csv files: {e}")

def mysql_db_connect():
    global mysql_username, mysql_password, mysql_host, mysql_database


    config = {
        "user": mysql_username,
        "password": mysql_password,
        "host": mysql_host,
        "raise_on_warnings": True
    }

    print(f"Connecting to mysql DB: {mysql_database}")

    try:
        conn = mysql.connector.connect(**config, use_pure=True)

        if conn.is_connected:
            print("Connected to MySQL DB")
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database}")
        else:
            return False

    except Exception as e:
        print(f"Could not connect to MYSQL DB: {e}")

if __name__ == "__main__":
    mysql_db_connect()
    fetch_data()