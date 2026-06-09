import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Optional Great Expectations
try:
    import great_expectations as ge
except ImportError:
    ge = None

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
QUARANTINE_DIR = os.getenv(
    "QUARANTINE_DIR_PATH",
    "/opt/airflow/data/quarantine"
)

REPORT_DIR = os.getenv(
    "REPORT_DIR_PATH",
    "/opt/airflow/data/reports"
)


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
        "total_amount"
    ]

    missing_cols = [c for c in required_columns if c not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # -----------------------------
    # BASIC REPORT
    # -----------------------------
    report = {
        "timestamp": str(datetime.now()),
        "total_records": len(df),
        "missing_customer_name": df["customer_name"].isnull().sum(),
        "missing_email": df["customer_email"].isnull().sum(),
        "missing_order_date": df["order_date"].isnull().sum(),
        "duplicate_orders": df["order_id"].duplicated().sum(),
        "negative_quantity": (df["quantity"] < 0).sum(),
        "negative_amount": (df["total_amount"] < 0).sum()
    }

    # -----------------------------
    # GREAT EXPECTATIONS (SAFE)
    # -----------------------------
    ge_results = {}

    if ge is not None:
        ge_df = ge.from_pandas(df)

        ge_results = {
            "not_null_order_id": ge_df.expect_column_values_to_not_be_null("order_id"),
            "not_null_customer_name": ge_df.expect_column_values_to_not_be_null("customer_name"),
            "unique_order_id": ge_df.expect_column_values_to_be_unique("order_id"),
            "valid_quantity": ge_df.expect_column_values_to_be_between(
                "quantity", min_value=0
            ),
            "valid_total_amount": ge_df.expect_column_values_to_be_between(
                "total_amount", min_value=0
            )
        }

    # -----------------------------
    # QUALITY SCORE
    # -----------------------------
    def compute_quality_score(report, ge_results):
        score = 100

        score -= report["missing_customer_name"] * 2
        score -= report["missing_email"] * 2
        score -= report["duplicate_orders"] * 5
        score -= report["negative_amount"] * 5
        score -= report["negative_quantity"] * 5

        for v in ge_results.values():
            if not v.get("success", True):
                score -= 10

        return max(score, 0)

    quality_score = compute_quality_score(report, ge_results)

    # -----------------------------
    # DECISION LOGIC
    # -----------------------------
    failed = (
        report["duplicate_orders"] > 0
        or report["missing_customer_name"] > 0
        or report["negative_amount"] > 0
        or quality_score < 70
    )

    status = "FAILED" if failed else "PASSED"

    # -----------------------------
    # REPORT OUTPUT
    # -----------------------------
    os.makedirs(REPORT_DIR, exist_ok=True)

    report_data = {
        "status": status,
        "quality_score": quality_score,
        "report": report,
        "ge_results": {k: v.get("success", None) for k, v in ge_results.items()},
        "timestamp": str(datetime.now())
    }

    report_path = os.path.join(
        REPORT_DIR,
        f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
            f"failed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        df.to_json(quarantine_path, orient="records", indent=4)

        print(f"Data quarantined at: {quarantine_path}")
        print("❌ VALIDATION FAILED")

        return {
            "status": status,
            "quality_score": quality_score,
            "report_path": report_path,
            "quarantine_path": quarantine_path
        }

    print("✅ VALIDATION PASSED")

    return {
        "status": status,
        "quality_score": quality_score,
        "report_path": report_path
    }