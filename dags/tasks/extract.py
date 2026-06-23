import requests
import json
import os
from dotenv import load_dotenv
import time
import logging
import psycopg2
from datetime import datetime

logger = logging.getLogger(__name__)
load_dotenv()

JSON_FILE_PATH = os.getenv(
    "JSON_FILE_PATH",
    "/opt/airflow/data/raw/orders_uncleaned.json"
)

METADATA_PATH = os.getenv(
    "METADATA_PATH",
    "/opt/airflow/data/metadata"
)

API_URL = "http://api:8000/orders"

MAX_RETRIES = 3
MAX_PAGES = 1000   # safety guard

def already_processed(file_name):
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

        cursor = conn.cursor()
        logger.info("Connection established")
    except Exception as e:
        logger.error("Database connection failed")
        logger.error(e)
        raise
    finally:
        cursor.close()
        conn.close()

    
    try:
        cursor.execute("""
            select 1 from processed_files
            where file_name =%s
        """,(file_name,))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error("Error fetching from database")
        logger.error(e)
    


def task_extract_data():

    if already_processed(JSON_FILE_PATH):
        logger.info("Skipping file")
        return

    all_data = []
    page = 1
    limit = 100

    start_time = datetime.utcnow()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")

    METADATA_FILE = os.path.join(
        METADATA_PATH,
        f"ingestion_{timestamp}.json"
    )

    # -------------------------
    # PAGINATION LOOP
    # -------------------------
    while True:

        if page > MAX_PAGES:
            logger.error("Max page limit reached. Stopping pagination.")
            break

        page_data = None

        # -------------------------
        # RETRY LOGIC (PER PAGE)
        # -------------------------
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Fetching page {page} (Attempt {attempt})")

                response = requests.get(
                    API_URL,
                    params={"page": page, "limit": limit},
                    timeout=10
                )

                response.raise_for_status()
                page_data = response.json()

                break

            except Exception as e:
                logger.warning(f"Attempt {attempt} failed for page {page}: {e}")

                if attempt < MAX_RETRIES:
                    logger.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logger.error(f"Page {page} failed after {MAX_RETRIES} attempts")
                    raise

        # -------------------------
        # STOP CONDITIONS
        # -------------------------
        if not page_data:
            logger.info(f"No more data at page {page}. Stopping pagination.")
            break

        if len(page_data) < limit:
            logger.info("Last page reached (partial page). Stopping pagination.")
            all_data.extend(page_data)
            break

        # -------------------------
        # COLLECT DATA
        # -------------------------
        all_data.extend(page_data)

        logger.info(f"Fetched {len(page_data)} records from page {page}")
        logger.info(f"Total records so far: {len(all_data)}")

        page += 1

    # -------------------------
    # SAVE RAW DATA
    # -------------------------
    try:
        os.makedirs(os.path.dirname(JSON_FILE_PATH), exist_ok=True)

        payload = {"data": all_data}

        with open(JSON_FILE_PATH, "w") as f:
            json.dump(payload, f, indent=4)

        logger.info(f"Raw data saved to: {JSON_FILE_PATH}")

    except Exception as e:
        logger.error(f"Error writing JSON file: {e}")
        raise

    # -------------------------
    # METADATA
    # -------------------------
    try:
        os.makedirs(METADATA_PATH, exist_ok=True)

        end_time = datetime.utcnow()

        metadata = {
            "pipeline": "ecommerce_pipeline",
            "task": "extract",
            "source": "vendor_api",
            "records_fetched": len(all_data),
            "status": "SUCCESS",
            "ingestion_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "pages_fetched": page
        }

        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=4)

        logger.info(f"Metadata saved to: {METADATA_FILE}")

    except Exception as e:
        logger.error(f"Error writing metadata: {e}")
        raise

    logger.info("Extraction completed successfully")

    return JSON_FILE_PATH