from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dotenv import load_dotenv
from tasks.extract import task_extract_data
from tasks.transform import task_transform_data
from tasks.validate import task_validate_data

load_dotenv()

PROCESSED_DATA_PATH = os.getenv(
    "PROCESSED_DATA_PATH",
    "/opt/airflow/data/processed"
)

OUTPUT_PATH = os.path.join(PROCESSED_DATA_PATH, "orders_cleaned.csv")

FAILED_FILE_PATH = os.getenv(
    "FAILED_DATA_PATH",
    "/opt/airflow/data/failed"
)

FAILED_PATH = os.path.join(FAILED_FILE_PATH, "failed.json")

JSON_PATH = os.getenv(
    "JSON_FILE_PATH",
    "/opt/airflow/data/raw/orders.json"
)

# ---------------- TASK FUNCTIONS ----------------

def extract_data():
    return task_extract_data()

def validate_data():
    task_validate_data(JSON_PATH)

def load_data(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="transform_data")

    output_file = data["output_file"]
    valid_rows = data["valid_rows"]

    print(output_file)
    print(valid_rows)

    return {
        "status": "success",
        "file": output_file,
        "rows": valid_rows
    }
# ---------------- DAG ----------------

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform_data,
        op_args=[JSON_PATH, OUTPUT_PATH, FAILED_PATH],
        do_xcom_push=True
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    # correct order
    extract_task >> transform_task >> validate_task >> load_task