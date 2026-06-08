import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

JSON_FILE_PATH = os.getenv(
    "JSON_FILE_PATH",
    "/opt/airflow/data/raw/orders_uncleaned.json"
)

API_URL = "http://api:8000/orders"


def task_extract_data():

    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        print(f"Fetched {len(data)} records")

    except Exception as e:
        print("Error fetching data from API")
        print(e)
        raise

    try:
        os.makedirs(
            os.path.dirname(JSON_FILE_PATH),
            exist_ok=True
        )

        payload = {
            "data": data
        }

        with open(JSON_FILE_PATH, "w") as f:
            json.dump(payload, f, indent=4)

        print(f"Raw data saved to: {JSON_FILE_PATH}")

        return JSON_FILE_PATH

    except Exception as e:
        print("Error writing JSON file")
        print(e)
        raise