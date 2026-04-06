from dotenv import load_dotenv
import os
import mysql.connector
from mysql.connector.plugins import caching_sha2_password

# global variables
load_dotenv()
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_host = os.getenv("MYSQL_HOST")
mysql_database = os.getenv("MYSQL_DATABASE")

def mysql_db_connect():
    global mysql_username, mysql_password, mysql_host, mysql_database


    config = {
        "user": mysql_username,
        "password": mysql_password,
        "host": mysql_host,
        "database": mysql_database,
        "raise_on_warnings": True
    }

    print(f"Connecting to mysql DB: {mysql_database}")

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