# test_mysqldb_connection.py

# import statements
from dotenv import load_dotenv
import os
import mysql.connector

# global variables
load_dotenv()
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_host = os.getenv("MYSQL_HOST")

def mysql_db_connect():
    global mysql_username, mysql_password, mysql_host

    config = {
        "user": mysql_username,
        "password": mysql_password,
        "host": mysql_host
    }

    print(f"Connecting to mysql DB {mysql_username}@{mysql_host}")

    try:
        conn = mysql.connector.connect(**config, use_pure=True)

        if conn.is_connected:
            print("Connected to MySQL DB")
            print("Closing connection")
            conn.close()

    except Exception as e:
        print(f"Could not connect to MYSQL DB: {e}")

if __name__ == "__main__":
    mysql_db_connect()