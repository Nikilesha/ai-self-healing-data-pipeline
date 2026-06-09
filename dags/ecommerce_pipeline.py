from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
from tasks.extract import task_extract_data
from tasks.transform import task_transform_data
#from tasks.validate import task_validate_data


load_dotenv()

# data paths from .env

PROCESSED_DATA_PATH = os.getenv(
    "PROCESSED_DATA_PATH",
    "/opt/airflow/data/processed"
)
OUTPUT_PATH = os.path.join(
    PROCESSED_DATA_PATH,
    "orders_cleaned.csv"
)

FAILED_FILE_PATH = os.getenv(
    "FAILED_DATA_PATH",
    "/opt/airflow/data/failed"
)

FAILED_PATH = os.path.join(
    FAILED_FILE_PATH,
    "failed.json"
)
JSON_PATH = os.getenv("JSON_FILE_PATH")

# extract, transform, validate functions
def extract_data():    
    task_extract_data()
    

def transform_data():
    task_transform_data(JSON_PATH,OUTPUT_PATH,FAILED_PATH)

def validate_data():
    pass

    
# define the DAG

with DAG(
    dag_id = "ecommerce_pipeline",
    start_date = datetime(2026,1,1),
    schedule = "@daily",
    catchup = False
) as dag:
    
    extract_task = PythonOperator(
        task_id = "extract_data",
        python_callable = extract_data
    )
    transform_task = PythonOperator(
        task_id = "transform_data",
        python_callable = transform_data
    )
    validate_task = PythonOperator(
        task_id = "validate_data",
        python_callable = validate_data
    )

    # set task dependencies
    extract_task >> transform_task >> validate_task

