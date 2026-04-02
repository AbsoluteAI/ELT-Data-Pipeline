from dotenv import load_dotenv
import os
import mysql.connector

# global variables
load_dotenv()
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_host = os.getenv("MYSQL_HOST")
mysql_database = os.getenv("MYSQL_DATABASE")

def mysql_db_connect():
    global mysql_username, mysql_password, mysql_host, mysql_database

    print(f"Connecting to mysql DB: {mysql_database}")

    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_username,
            password=mysql_password,
            database=mysql_database
        )

        print("Connected to MySQL DB")

    except Exception as e:
        print(f"Could not connect to MYSQL DB: {e}")

if __name__ == "__main__":
    mysql_db_connect()