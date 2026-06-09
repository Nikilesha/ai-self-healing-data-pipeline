import requests
import json
import os
from dotenv import load_dotenv
import time


load_dotenv()

JSON_FILE_PATH = os.getenv(
    "JSON_FILE_PATH",
    "/opt/airflow/data/raw/orders_uncleaned.json"
)

API_URL = "http://api:8000/orders"

MAX_RETRIES = 3


def task_extract_data():
    data = None

    for attempt in range(1,MAX_RETRIES+1):
        try:
            print(f"Connecting to API attempt {attempt}/{MAX_RETRIES}")
            response = requests.get(API_URL,timeout=10)
            response.raise_for_status()

            data = response.json()
            print(f"fetched {len(data)} records")
            break;
        except Exception as e:
            print(f"Attempt {attempt} failed")
            print(e)

            if attempt < MAX_RETRIES:
                print("Waiting for 5 seconds before retrying...")
                time.sleep(5)

            if attempt == MAX_RETRIES:
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