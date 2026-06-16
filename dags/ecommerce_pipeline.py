from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv
from airflow.utils.trigger_rule import TriggerRule

from tasks.extract import task_extract_data
from tasks.transform import task_transform_data
from tasks.validate import task_validate_data
from tasks.load import task_load_data
from utils.checkpoint import mark_done, is_done

# ---------------- LOGGING ----------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------- ENV ----------------
load_dotenv()

PROCESSED_DATA_PATH = os.getenv(
    "PROCESSED_DATA_PATH",
    "/opt/airflow/data/processed"
)

FAILED_FILE_PATH = os.getenv(
    "FAILED_DATA_PATH",
    "/opt/airflow/data/failed"
)

JSON_PATH = os.getenv(
    "JSON_FILE_PATH",
    "/opt/airflow/data/raw/orders.json"
)

OUTPUT_PATH = os.path.join(PROCESSED_DATA_PATH, "orders_cleaned.csv")
FAILED_PATH = os.path.join(FAILED_FILE_PATH, "failed.json")

# ---------------- DEFAULT ARGS ----------------
default_args = {
    "owner": "nikil",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

# ---------------- WRAPPERS ----------------

def extract_wrapper():
    if is_done("extract"):
        logger.info("Skipping extract (already completed)")
        return "SKIPPED"

    try:
        result = task_extract_data()
        mark_done("extract", {"status": "success"})
        return result
    except Exception:
        logger.exception("Extract failed")
        raise


def transform_wrapper():
    if is_done("transform"):
        logger.info("Skipping transform (already completed)")
        return "SKIPPED"

    try:
        result = task_transform_data(JSON_PATH, OUTPUT_PATH, FAILED_PATH)

        # store useful metadata (better for debugging/recovery)
        mark_done("transform", {
            "status": "success",
            "valid_rows": result.get("valid_rows"),
            "invalid_rows": result.get("invalid_rows"),
            "output_file": OUTPUT_PATH
        })

        return result

    except Exception:
        logger.exception("Transform failed")
        raise


def validate_wrapper():
    if is_done("validate"):
        logger.info("Skipping validate (already completed)")
        return "SKIPPED"

    try:
        result = task_validate_data(OUTPUT_PATH)

        mark_done("validate", {
            "status": "success",
            "output_file": OUTPUT_PATH
        })

        return result

    except Exception:
        logger.exception("Validation failed")
        raise


def load_wrapper():
    if is_done("load"):
        logger.info("Skipping load (already completed)")
        return "SKIPPED"

    try:
        result = task_load_data()

        mark_done("load", {"status": "success"})
        return result

    except Exception:
        logger.exception("Load failed")
        raise


def generate_report():
    logger.info("Generating pipeline report...")


# ---------------- DAG ----------------

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    # ---------------- TASKS ----------------

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_wrapper
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_wrapper
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_wrapper,
        retries=2
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_wrapper
    )

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # ---------------- DEPENDENCIES ----------------

    extract_task >> transform_task >> validate_task >> load_data_task
    load_data_task >> generate_report_task