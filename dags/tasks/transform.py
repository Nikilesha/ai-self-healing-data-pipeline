import json
import pandas as pd
import os


def task_transform_data(input_file, output_file):

    # Read JSON payload
    with open(input_file, "r") as f:
        payload = json.load(f)

    # Extract records into DataFrame
    df = pd.DataFrame(payload["data"])

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Remove duplicate orders
    df.drop_duplicates(
        subset=["order_id"],
        inplace=True
    )

    # Fill missing customer names
    df["customer_name"] = df["customer_name"].fillna(
        "unknown customer"
    )

    # Fill missing emails
    df["customer_email"] = df["customer_email"].fillna(
        "missing@email.com"
    )

    # Fill missing order status
    df["order_status"] = df["order_status"].fillna(
        "pending"
    )

    # Convert date column
    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce"
    )

    # Create output directory if needed
    os.makedirs(
        os.path.dirname(output_file),
        exist_ok=True
    )

    # Save processed file
    df.to_csv(output_file, index=False)

    print(f"Processed file saved to {output_file}")

    return output_file