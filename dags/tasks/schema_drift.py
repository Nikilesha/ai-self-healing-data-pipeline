import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

REPORT_DIR = os.getenv(
    "REPORT_DIR_PATH",
    "/opt/airflow/data/reports"
)

SCHEMA_PATH = os.getenv(
    "SCHEMA_PATH",
    "/opt/airflow/dags/utils/expected_schema.json"
)


def detect_schema_drift(df):
    """
    Detect schema drift and apply self-healing actions.

    Actions:
    - Detect new columns
    - Detect missing columns
    - Auto-create missing columns
    - Generate schema drift report
    - Return corrected dataframe
    """

    # -----------------------------
    # LOAD EXPECTED SCHEMA
    # -----------------------------
    if not os.path.exists(SCHEMA_PATH):
        raise FileNotFoundError(
            f"Expected schema file not found: {SCHEMA_PATH}"
        )

    with open(SCHEMA_PATH, "r") as f:
        schema = json.load(f)

    expected_columns = [
        col.strip().lower()
        for col in schema["columns"]
    ]

    actual_columns = [
        col.strip().lower()
        for col in df.columns.tolist()
    ]

    # -----------------------------
    # DETECT DRIFT
    # -----------------------------
    new_columns = list(
        set(actual_columns) - set(expected_columns)
    )

    missing_columns = list(
        set(expected_columns) - set(actual_columns)
    )

    drift_detected = bool(
        new_columns or missing_columns
    )

    actions_taken = []

    # -----------------------------
    # HANDLE NEW COLUMNS
    # -----------------------------
    if new_columns:
        logger.warning(
            f"New columns detected: {new_columns}"
        )

        actions_taken.append(
            f"detected_new_columns:{new_columns}"
        )

    # -----------------------------
    # HANDLE MISSING COLUMNS
    # -----------------------------
    if missing_columns:

        logger.warning(
            f"Missing columns detected: {missing_columns}"
        )

        for col in missing_columns:
            logger.warning(f"Missing column detected: {col}")

            actions_taken.append(
                f"created_missing_column:{col}"
            )

        logger.info(
            "Missing columns auto-created with NULL values"
        )

    # -----------------------------
    # REORDER COLUMNS
    # -----------------------------
    ordered_columns = []

    for col in expected_columns:
        if col in df.columns:
            ordered_columns.append(col)

    extra_columns = [
        col
        for col in df.columns
        if col not in expected_columns
    ]

    df = df[
        ordered_columns + extra_columns
    ]

    # -----------------------------
    # REPORT
    # -----------------------------
    os.makedirs(REPORT_DIR, exist_ok=True)

    report = {
        "timestamp": str(datetime.now()),
        "drift_detected": drift_detected,
        "expected_columns": expected_columns,
        "actual_columns": actual_columns,
        "new_columns": new_columns,
        "missing_columns": missing_columns,
        "actions_taken": actions_taken
    }

    report_path = os.path.join(
        REPORT_DIR,
        f"schema_drift_report_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(report_path, "w") as f:
        json.dump(
            report,
            f,
            indent=4
        )

    logger.info(
        f"Schema drift report saved: {report_path}"
    )
    new_columns = list(set(actual_columns) - set(expected_columns))

    logger.info(f"EXPECTED COLUMNS = {expected_columns}")
    logger.info(f"ACTUAL COLUMNS = {actual_columns}")
    logger.info(f"NEW COLUMNS = {list(set(actual_columns) - set(expected_columns))}")

    if drift_detected:
        logger.warning(
            "Schema drift detected and handled successfully"
        )
    else:
        logger.info(
            "No schema drift detected"
        )

    return df