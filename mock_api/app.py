from fastapi import FastAPI
import os
import pandas as pd
import numpy as np

app = FastAPI()

CSV_PATH = os.getenv(
    "CSV_PATH",
    "/opt/airflow/data/raw/orders_uncleaned.csv"
)

@app.get("/")
def home():
    return {
        "status": "Vendor API Running",
        "inside_docker": True
    }

@app.get("/orders")
def get_orders(page: int = 1 ,limit: int = 100):

    df = pd.read_csv(CSV_PATH)

    # Replace inf values
    df = df.replace([np.inf, -np.inf], np.nan)

    # Convert NaN to None for valid JSON
    df = df.where(pd.notnull(df), None)

    start = (page - 1) * limit
    end = start + limit

    return df.iloc[start:end].to_dict(orient="records")