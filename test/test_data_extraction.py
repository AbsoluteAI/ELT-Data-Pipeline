# test_data_extraction.py

# import statements
import os
from dotenv import load_dotenv
from gridstatusio import GridStatusClient

# Recommended: set GRIDSTATUS_API_KEY as an
# environment variable instead of hardcoding
def extract_api_data():
    load_dotenv()
    gridstatus_api_key = os.getenv("GRIDSTATUS_API_KEY")

    try:
        print("Connecting to API...")
        client = GridStatusClient(gridstatus_api_key)

        print("Attempting to extract data...")

        # Fetch data as pandas DataFrame
        df = client.get_dataset(
          dataset="aeso_supply_and_demand",
          start="2026-03-22",
          end="2026-03-25",
          timezone="market",
        )

        print(f"Data extractions successful.\nPrinting contents:\n{df.head()}")

        return True

    except Exception as e:
        print(f"Data extraction failed.\nError: {e}")

        return False

if __name__ == "__main__":
    extract_api_data()