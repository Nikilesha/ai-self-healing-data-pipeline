from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

CSV_PATH = '/opt/airflow/data/raw/orders.csv'

def extract_data():
    df = pd.read_csv(CSV_PATH)
    print(df.head())

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