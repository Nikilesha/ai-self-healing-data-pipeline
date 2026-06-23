import pandas as pd
import os
import json
from datetime import datetime
import logging
from tasks.schema_drift import detect_schema_drift

logger = logging.getLogger(__name__)

# -----------------------------
# OPTIONAL GREAT EXPECTATIONS
# -----------------------------
try:
    import great_expectations as ge
except ImportError:
    ge = None


# -----------------------------
# ENV PATHS
# -----------------------------
QUARANTINE_DIR = os.getenv("QUARANTINE_DIR_PATH", "/opt/airflow/data/quarantine")
REPORT_DIR = os.getenv("REPORT_DIR_PATH", "/opt/airflow/data/reports")
VALIDATED_DIR = os.getenv("VALIDATED_DATA_PATH", "/opt/airflow/data/validated")


# -----------------------------
# SAFE JSON SERIALIZER
# -----------------------------
def to_python(obj):
    import numpy as np

    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, dict):
        return {k: to_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_python(i) for i in obj]
    return obj


# -----------------------------
# DATA LOADER (CSV + JSON SAFE)
# -----------------------------
def load_dataframe(file_path):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)

    with open(file_path, "r") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "data" in raw:
        return pd.DataFrame(raw["data"])
    elif isinstance(raw, list):
        return pd.DataFrame(raw)
    elif isinstance(raw, dict):
        return pd.DataFrame([raw])
    else:
        raise ValueError("Unsupported file format")


# -----------------------------
# MAIN VALIDATION FUNCTION
# -----------------------------
def task_validate_data(file_path):

    df = load_dataframe(file_path)

    df.columns = df.columns.str.strip().str.lower()

    logger.info(f"ACTUAL COLUMNS = {df.columns.tolist()}")

    df = detect_schema_drift(df)

    logger.info(f"Validation started | Shape: {df.shape}")

    logger.info(f"Validation started | Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")

    # ---------------- REQUIRED COLUMNS ----------------
    required_columns = [
        "order_id",
        "customer_name",
        "customer_email",
        "order_date",
        "quantity",
        "total_amount",
    ]

    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        logger.error("Columns missing")
        raise ValueError(f"Missing required columns: {missing_cols}")

    # ---------------- TYPE CLEANING ----------------
    df["order_id"] = (
        df["order_id"].fillna("").astype(str).str.strip().replace("nan", "")
    )

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    logger.info("Type cleaning completed")

    # ---------------- METRICS ----------------
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=180)
    stale_records = (df["order_date"] < cutoff_date).sum()

    datatype_errors = {
        "invalid_order_id": (~df["order_id"].str.match(r"^ORD\d+$", na=False)).sum(),
        "invalid_quantity": df["quantity"].isna().sum(),
        "invalid_total_amount": df["total_amount"].isna().sum(),
        "invalid_order_date": df["order_date"].isna().sum(),
    }

    report = {
        "timestamp": str(datetime.now()),
        "total_records": len(df),
        "missing_customer_name": df["customer_name"].isnull().sum(),
        "missing_email": df["customer_email"].isnull().sum(),
        "missing_order_date": df["order_date"].isnull().sum(),
        "duplicate_orders": df["order_id"].duplicated().sum(),
        "negative_quantity": (df["quantity"] < 0).sum(),
        "negative_amount": (df["total_amount"] < 0).sum(),
        **datatype_errors,
        "stale_records": stale_records,
    }

    # ---------------- QUALITY SCORE ----------------
    total = max(report["total_records"], 1)

    completeness = 100 - (
        (report["missing_customer_name"]
         + report["missing_email"]
         + report["missing_order_date"]) / total * 100
    )

    uniqueness = 100 - (report["duplicate_orders"] / total * 100)

    validity = 100 - (
        (report["invalid_order_id"]
         + report["invalid_quantity"]
         + report["invalid_total_amount"]
         + report["invalid_order_date"]) / total * 100
    )

    freshness = 100 - (report["stale_records"] / total * 100)

    completeness = max(completeness, 0)
    uniqueness = max(uniqueness, 0)
    validity = max(validity, 0)
    freshness = max(freshness, 0)

    quality_score = round(
        completeness * 0.30 +
        uniqueness * 0.25 +
        validity * 0.30 +
        freshness * 0.15,
        2
    )

    if quality_score >= 90:
        health_status = "EXCELLENT"
    elif quality_score >= 75:
        health_status = "GOOD"
    elif quality_score >= 60:
        health_status = "WARNING"
    else:
        health_status = "CRITICAL"

    logger.info("Health conditions validated")

    # ---------------- FAILURE CONDITION ----------------
    failed = (
        quality_score < 70
        or report["negative_amount"] > 50
        or report["invalid_total_amount"] > 50
        or report["invalid_quantity"] > 50
        or report["invalid_order_date"] > 50
    )

    # ---------------- GE (OPTIONAL) ----------------
    ge_results = {}
    if ge is not None:
        ge_df = ge.from_pandas(df)

        ge_results = {
            "not_null_order_id": ge_df.expect_column_values_to_not_be_null("order_id").success,
            "not_null_customer_name": ge_df.expect_column_values_to_not_be_null("customer_name").success,
            "unique_order_id": ge_df.expect_column_values_to_be_unique("order_id").success,
            "valid_quantity": ge_df.expect_column_values_to_be_between("quantity", min_value=0).success,
            "valid_total_amount": ge_df.expect_column_values_to_be_between("total_amount", min_value=0).success,
        }

    # ---------------- REPORT OUTPUT ----------------
    os.makedirs(REPORT_DIR, exist_ok=True)

    report_data = {
        "quality_score": quality_score,
        "health_status": health_status,
        "report": report,
        "ge_results": ge_results,
        "timestamp": str(datetime.now()),
    }

    report_path = os.path.join(
        REPORT_DIR,
        f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(report_path, "w") as f:
        json.dump(to_python(report_data), f, indent=4)

    logger.info(f"Validation report saved at {report_path}")

    # ---------------- QUARANTINE ----------------
    if failed:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"failed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        df.to_json(quarantine_path, orient="records", indent=4)

        logger.error("Validation FAILED")
        raise Exception(f"Dataset failed validation | Score: {quality_score}")

    # ---------------- SAVE VALIDATED DATA ----------------
    os.makedirs(VALIDATED_DIR, exist_ok=True)

    validated_path = os.path.join(VALIDATED_DIR, "orders_validated.csv")

    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(validated_path, index=False)

    logger.info(f"Validation PASSED | Saved at {validated_path}")

    # ---------------- RETURN ----------------
    return {
        "quality_score": quality_score,
        "health_status": health_status,
        "report_path": report_path,
        "validated_path": validated_path,
    }