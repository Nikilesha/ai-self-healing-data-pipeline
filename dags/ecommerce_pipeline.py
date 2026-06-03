from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

# data paths from .env
CSV_PATH = os.getenv("RAW_DATA_PATH")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")
PROCESSED_DATA_PATH_FILENAME = os.getenv("PROCESSED_DATA_PATH_FILENAME")
OUTPUT_PATH = os.path.join(PROCESSED_DATA_PATH,PROCESSED_DATA_PATH_FILENAME)

# extract, transform, validate functions
def extract_data():
    df = pd.read_csv(CSV_PATH)
    print(df.head())

def transform_data():
    print("PROCESSED_DATA_PATH =", OUTPUT_PATH)

    df = pd.read_csv(CSV_PATH)

    df.columns = (df.columns.str.strip().str.lower())

    df.to_csv(OUTPUT_PATH, index=False)
    print(df)

def validate_data():
    df = pd.read_csv(OUTPUT_PATH)

    null_counts = df.isnull().sum()
    print(null_counts)

    
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
