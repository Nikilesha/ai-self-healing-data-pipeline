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

        # ---------------- VALIDATE BASIC STRUCTURE ----------------
        if df.empty:
            raise ValueError("Empty dataset received")

        required_columns = [
            "order_id",
            "customer_name",
            "customer_email",
            "order_status",
            "order_date",
            "quantity",
            "unit_price",
            "discount_percentage",
            "shipping_cost",
            "total_amount"
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # ---------------- TYPE CONVERSION (BEFORE SPLIT) ----------------
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        numeric_cols = [
            "quantity",
            "unit_price",
            "discount_percentage",
            "shipping_cost",
            "total_amount"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # ---------------- FAILURE ISOLATION (IMPORTANT FIX) ----------------
        invalid_mask = (
            df["order_date"].isna() |
            df["quantity"].isna() |
            df["unit_price"].isna() |
            df["discount_percentage"].isna() |
            df["shipping_cost"].isna() |
            df["total_amount"].isna()
        )

        invalid_df = df[invalid_mask].copy()
        valid_df = df[~invalid_mask].copy()

        logger.info(f"Valid rows after type validation: {len(valid_df)}")
        logger.info(f"Invalid rows isolated: {len(invalid_df)}")

        # ---------------- CLEANING (ONLY VALID DATA) ----------------
        valid_df.drop_duplicates(subset=["order_id"], inplace=True)

        valid_df["customer_name"] = valid_df["customer_name"].fillna("unknown customer")
        valid_df["customer_email"] = valid_df["customer_email"].fillna("missing@email.com")
        valid_df["order_status"] = valid_df["order_status"].fillna("pending")

        valid_df["customer_name"] = valid_df["customer_name"].str.strip().str.title()
        valid_df["customer_email"] = valid_df["customer_email"].str.strip().str.lower()
        valid_df["order_status"] = valid_df["order_status"].str.strip().str.title()

        # ---------------- BUSINESS RULE FILTERING ----------------
        valid_df = valid_df[
            (valid_df["quantity"] > 0) &
            (valid_df["unit_price"] > 0) &
            (valid_df["discount_percentage"].between(0, 1))
        ]

        # ---------------- FEATURE ENGINEERING ----------------
        valid_df["calculated_total"] = (
            valid_df["quantity"] * valid_df["unit_price"] *
            (1 - valid_df["discount_percentage"]) +
            valid_df["shipping_cost"]
        )

        valid_df["amount_mismatch"] = (
            abs(valid_df["total_amount"] - valid_df["calculated_total"]) > 1
        )

        valid_df["revenue"] = valid_df["quantity"] * valid_df["unit_price"]
        valid_df["net_amount"] = valid_df["total_amount"] - valid_df["shipping_cost"]
        valid_df["order_month"] = valid_df["order_date"].dt.to_period("M")
        valid_df["is_high_value"] = valid_df["total_amount"] > valid_df["total_amount"].quantile(0.9)

        logger.info(f"Mismatched totals: {valid_df['amount_mismatch'].sum()}")

        # ---------------- SAVE FILES ----------------
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        os.makedirs(os.path.dirname(failed_file), exist_ok=True)

        valid_df.to_csv(output_file, index=False)

        if not invalid_df.empty:
            invalid_df.to_csv(failed_file, index=False)

        # ---------------- LOGGING ----------------
        logger.info(f"Processed file saved to {output_file}")
        logger.info(f"Failed records saved to {failed_file}")
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