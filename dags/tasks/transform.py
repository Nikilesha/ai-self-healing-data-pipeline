import json
import pandas as pd
import os
import shutil
import logging

logger = logging.getLogger(__name__)


def task_transform_data(input_file, output_file, failed_file):

    # ---------------- LOAD JSON ----------------
    try:
        with open(input_file, "r") as f:
            payload = json.load(f)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        raise

    try:
        # ---------------- DATAFRAME ----------------
        df = pd.DataFrame(payload["data"])
        logger.info(f"Initial records loaded: {len(df)}")

        # ---------------- CLEAN COLUMN NAMES ----------------
        df.columns = df.columns.str.strip().str.lower()
        logger.info(f"Columns found: {df.columns.tolist()}")

        # ---------------- VALIDATE SCHEMA ----------------
        required_columns = [
            "order_id",
            "customer_name",
            "customer_email",
            "order_status",
            "order_date"
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        if df.empty:
            raise ValueError("Empty dataset received")

        # ---------------- BASIC CLEANING ----------------
        df.drop_duplicates(subset=["order_id"], inplace=True)

        df["customer_name"] = df["customer_name"].fillna("unknown customer")
        df["customer_email"] = df["customer_email"].fillna("missing@email.com")
        df["order_status"] = df["order_status"].fillna("pending")

        df["customer_name"] = df["customer_name"].str.strip().str.title()
        df["customer_email"] = df["customer_email"].str.strip().str.lower()
        df["order_status"] = df["order_status"].str.strip().str.title()

        # ---------------- TYPE CONVERSION ----------------
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df["discount_percentage"] = pd.to_numeric(df["discount_percentage"], errors="coerce")
        df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce")

        invalid_dates = df["order_date"].isna().sum()
        logger.warning(f"Invalid dates found: {invalid_dates}")

        # ---------------- BUSINESS RULES ----------------
        df = df[
            (df["quantity"] > 0) &
            (df["unit_price"] > 0) &
            (df["discount_percentage"].between(0, 1))
        ]

        # ---------------- CALCULATION ----------------
        df["calculated_total"] = (
            df["quantity"] * df["unit_price"] *
            (1 - df["discount_percentage"]) +
            df["shipping_cost"]
        )

        df["amount_mismatch"] = abs(df["total_amount"] - df["calculated_total"]) > 1

        logger.info(f"Mismatched totals: {df['amount_mismatch'].sum()}")

        # ---------------- SPLIT VALID / INVALID ----------------
        valid_df = df[df["order_date"].notna()].copy()
        invalid_df = df[df["order_date"].isna()].copy()

        # ---------------- FEATURE ENGINEERING (ONLY VALID) ----------------
        valid_df["revenue"] = valid_df["quantity"] * valid_df["unit_price"]
        valid_df["net_amount"] = valid_df["total_amount"] - valid_df["shipping_cost"]
        valid_df["order_month"] = valid_df["order_date"].dt.to_period("M")
        valid_df["is_high_value"] = valid_df["total_amount"] > valid_df["total_amount"].quantile(0.9)

        # ---------------- SAVE FILES ----------------
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        valid_df.to_csv(output_file, index=False)

        if len(invalid_df) > 0:
            invalid_df.to_csv(failed_file, index=False)

        # ---------------- LOGGING ----------------
        logger.info(f"Processed file saved to {output_file}")
        logger.info("Transformation complete")
        logger.info(f"Valid rows: {len(valid_df)}")
        logger.info(f"Invalid rows: {len(invalid_df)}")

        # ---------------- RETURN METRICS ----------------
        return {
            "output_file": output_file,
            "failed_file": failed_file,
            "valid_rows": len(valid_df),
            "invalid_rows": len(invalid_df)
        }

    except Exception as e:
        os.makedirs(os.path.dirname(failed_file), exist_ok=True)

        shutil.copy(input_file, failed_file)

        logger.error(f"Transformation failed: {e}")
        logger.error(f"Copied raw file to failed path: {failed_file}")

        raise