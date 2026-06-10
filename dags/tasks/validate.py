import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Optional Great Expectations
try:
    import great_expectations as ge
except ImportError:
    ge = None

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
QUARANTINE_DIR = os.getenv("QUARANTINE_DIR_PATH", "/opt/airflow/data/quarantine")

REPORT_DIR = os.getenv("REPORT_DIR_PATH", "/opt/airflow/data/reports")


# -----------------------------
# SAFE JSON LOADER
# -----------------------------
def load_dataframe(file_path):
    with open(file_path, "r") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "data" in raw:
        df = pd.DataFrame(raw["data"])
    elif isinstance(raw, list):
        df = pd.DataFrame(raw)
    elif isinstance(raw, dict):
        df = pd.DataFrame([raw])
    else:
        raise ValueError("Unsupported JSON structure")

    return df


# -----------------------------
# JSON SERIALIZATION FIX
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
# MAIN VALIDATION FUNCTION
# -----------------------------
def task_validate_data(file_path):

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    df = load_dataframe(file_path)

    # -----------------------------
    # NORMALIZE COLUMNS
    # -----------------------------
    df.columns = df.columns.str.strip().str.lower()

    print("DF SHAPE:", df.shape)
    print("DF COLUMNS:", df.columns.tolist())

    # -----------------------------
    # REQUIRED COLUMNS CHECK
    # -----------------------------
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
        raise ValueError(f"Missing required columns: {missing_cols}")

    # -----------------------------
    # DATATYPE VALIDATION
    # -----------------------------

    df["order_id"] = (
        df["order_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("nan","")
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    df["total_amount"] = pd.to_numeric(
        df["total_amount"],
        errors="coerce"
    )

    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce"
    )

    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=180)

    stale_records = (
        df["order_date"] < cutoff_date
    ).sum()

    datatype_errors = {
        "invalid_order_id": (
            ~df["order_id"].str.match(r"^ORD\d+$", na=False)
        ).sum(),

        "invalid_quantity": df["quantity"].isna().sum(),

        "invalid_total_amount": df["total_amount"].isna().sum(),

        "invalid_order_date": df["order_date"].isna().sum()
    }

    # -----------------------------
    # BASIC REPORT
    # -----------------------------
    print("Unique Order IDs:", df["order_id"].nunique())
    print("Duplicate Order IDs:", df["order_id"].duplicated().sum())
    print(
        "Invalid Order IDs:",
        (~df["order_id"].str.match(r"^ORD\d+$", na=False)).sum()
    )


    report = {
        "timestamp": str(datetime.now()),
        "total_records": len(df),
        "missing_customer_name": df["customer_name"].isnull().sum(),
        "missing_email": df["customer_email"].isnull().sum(),
        "missing_order_date": df["order_date"].isnull().sum(),
        "duplicate_orders": df["order_id"].duplicated().sum(),
        "negative_quantity": (df["quantity"] < 0).sum(),
        "negative_amount": (df["total_amount"] < 0).sum(),
        "invalid_order_id":datatype_errors["invalid_order_id"],
        "invalid_quantity":datatype_errors["invalid_quantity"],
        "invalid_total_amount":datatype_errors["invalid_total_amount"],
        "invalid_order_date":datatype_errors["invalid_order_date"],
        "stale_records": stale_records,
    }

    # -----------------------------
    # GREAT EXPECTATIONS (SAFE)
    # -----------------------------
    ge_results = {}

    if ge is not None:
        ge_df = ge.from_pandas(df)

        ge_results = {
            "not_null_order_id": ge_df.expect_column_values_to_not_be_null("order_id"),
            "not_null_customer_name": ge_df.expect_column_values_to_not_be_null(
                "customer_name"
            ),
            "unique_order_id": ge_df.expect_column_values_to_be_unique("order_id"),
            "valid_quantity": ge_df.expect_column_values_to_be_between(
                "quantity", min_value=0
            ),
            "valid_total_amount": ge_df.expect_column_values_to_be_between(
                "total_amount", min_value=0
            ),
        }

    # -----------------------------
    # QUALITY SCORE
    # -----------------------------
    def compute_weighted_quality_score(report):
        total_records = max(report["total_records"], 1)

        completeness_score = (
            100
            - (
                report["missing_customer_name"]
                + report["missing_email"]
                + report["missing_order_date"]
            )
            / total_records
            * 100
        )

        uniqueness_score = (
            100
            - (report["duplicate_orders"] / total_records * 100)
        )

        validity_score = (
            100
            - (
                report["invalid_order_id"]
                + report["invalid_quantity"]
                + report["invalid_total_amount"]
                + report["invalid_order_date"]
            )
            / total_records
            * 100
        )

        freshness_score = (
            100
            - (report["stale_records"] / total_records * 100)
        )

        #makes sure it is not negative 
        completeness_score = max(completeness_score, 0)
        uniqueness_score = max(uniqueness_score, 0)
        validity_score = max(validity_score, 0)
        freshness_score = max(freshness_score, 0)


        quality_score = (
            completeness_score * 0.30 +
            uniqueness_score * 0.25 +
            validity_score * 0.30 +
            freshness_score * 0.15
        )

        return round(quality_score,2)

    quality_score = compute_weighted_quality_score(report)

    if quality_score >= 90:
        health_status = "EXCELLENT"

    elif quality_score >= 75:
        health_status = "GOOD"

    elif quality_score >= 60:
        health_status = "WARNING"

    else:
        health_status = "CRITICAL"
    
    # -----------------------------
    # DECISION LOGIC
    # -----------------------------
    failed = (
        quality_score < 70
        or report["negative_amount"] > 50
        or report["invalid_total_amount"] > 50
        or report["invalid_quantity"] > 50
    )

    status = "FAILED" if failed else "PASSED"

    unhealthy_dataset = (
        health_status == "WARNING"
        or health_status == "CRITICAL"
    )

    # -----------------------------
    # REPORT OUTPUT
    # -----------------------------
    os.makedirs(REPORT_DIR, exist_ok=True)

    report_data = {
        "status": status,
        "quality_score": quality_score,
        "health_status" : health_status,
        "unhealthy_dataset": unhealthy_dataset,
        "report": report,
        "ge_results": {k: v.get("success", None) for k, v in ge_results.items()},
        "timestamp": str(datetime.now()),
        
    }

    report_path = os.path.join(
        REPORT_DIR, f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(report_path, "w") as f:
        json.dump(to_python(report_data), f, indent=4)

    print(f"Validation report saved at: {report_path}")

    # -----------------------------
    # QUARANTINE
    # -----------------------------
    if failed:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"failed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )

        df.to_json(quarantine_path, orient="records", indent=4)

        print(f"Data quarantined at: {quarantine_path}")
        print("❌ VALIDATION FAILED")

        return {
            "status": status,
            "quality_score": quality_score,
            "report_path": report_path,
            "quarantine_path": quarantine_path,
        }

    print("✅ VALIDATION PASSED")

    return {
        "status": status,
        "quality_score": quality_score,
        "health_status": health_status,
        "unhealthy_dataset": unhealthy_dataset,
        "report_path": report_path,
    }