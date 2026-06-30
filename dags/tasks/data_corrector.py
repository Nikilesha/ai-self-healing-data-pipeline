import pandas as pd
import logging


logger = logging.getLogger(__name__)

EXPECTED_TYPES = {
    "order_id": "string",
    "customer_name": "string",
    "customer_email": "string",
    "order_date": "datetime",
    "quantity": "int",
    "total_amount": "float",
}


def clean_string(series):
    return(
        series.fillna("")
        .astype(str)
        .str.strip()
    )
def clean_datetime(series):
        return pd.to_datetime(series,errors="coerce")

def clean_integer(series):
    # Convert everything to string and remove spaces
    series = series.astype(str).str.strip()

    # Extract only the numeric portion
    series = series.str.extract(r"(\d+\.?\d*)")[0]

    # Convert to numeric
    series = pd.to_numeric(series, errors="coerce")
    series = series.fillna(pd.NA).astype("Int64")

    return series

def clean_float(series):
    #convert to string
    series = series.astype(str).str.strip()

    #remove commas if present
    series = series.str.replace(",","",regex=False)

    #remove currecny symbols
    series = series.str.replace(r"[^\d.]","",regex = True)

    #convert to numeric
    series = pd.to_numeric(series,errors="coerce")

    return series

def correct_datatypes(df):
    df = df.copy()
    summary = {
        "columns_processed": 0,
        "corrections": {}
    }
    for column, datatype in EXPECTED_TYPES.items():
        if column not in df.columns:
            continue
        if datatype == "string":
            df[column] = clean_string(df[column])

        elif datatype == "int":
            df[column] = clean_integer(df[column])

        elif datatype == "float":
            df[column] = clean_float(df[column])

        elif datatype == "datetime":
            df[column] = clean_datetime(df[column])

        summary["columns_processed"] += 1
        summary["corrections"][column] = datatype

        logger.info(f"Applied {datatype} correction on column '{column}'")

    return df,summary