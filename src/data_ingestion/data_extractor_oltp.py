# data_extractor_oltp.py

# license requires attribution
# url for future reference:
# https://www.kaggle.com/datasets/noahtaylson/elite-retail-transaction-dataset-uk?select=supplier_directory.csv

# import statements
import oracledb

# global variables

# module functions

def oracle_db_connect():

    try:
        conn = oracledb.connect(user=USERNAME,
            password=PASSWORD,
            dsn="localhost:1521/my_db")

        with conn.cursor() as cur:
            cur.execute("SELECT 'Hello World!' FROM dual")
            res = cur.fetchall()

            print(res)

    except Exception as e:
        print(f"Could not connect to Oracle DB: {e}")


if __name__ == "__main__":
    oracle_db_connect()