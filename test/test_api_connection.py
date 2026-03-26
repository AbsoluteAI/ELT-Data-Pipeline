# test_api_connection.py

# import statements
import requests
import os
from dotenv import load_dotenv

# global variables
load_dotenv()
gridstatus_api_key = os.getenv("GRIDSTATUS_API_KEY")

def test_api_connection():
    global gridstatus_api_key

    url = f"https://api.gridstatus.io/v1/datasets/aeso_supply_and_demand/query?start_time=2026-03-24&end_time=2026-03-27&timezone=market&api_key={gridstatus_api_key}"

    print("Connecting to data source...")
    try:
        response = requests.get(url, stream=True, timeout=10)

        if response.status_code == 200:
            print("Connection successful\n")
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

if __name__ == "__main__":
    test_api_connection()