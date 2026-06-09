import json
import pandas as pd
import os
import shutil

def task_transform_data(input_file, output_file, failed_file):

    
    # Read JSON payload
    try:
        with open(input_file, "r") as f:
            payload = json.load(f)
    except Exception as e:
        print(e)
        print(f"Error reading input files:\n {e}")
        raise

    try:
        # Extract records into DataFrame
        df = pd.DataFrame(payload["data"])

        # Standardize column names
        df.columns = df.columns.str.strip().str.lower()

        required_columns = [
            "order_id",
            "customer_name",
            "customer_email",
            "order_status",
            "order_date"
        ]

        missing_columns = []

        for col in required_columns:
            if col not in df.columns:
                missing_columns.append(col)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # Remove duplicate orders
        df.drop_duplicates(subset=["order_id"], inplace=True)

        # Fill missing customer names
        df["customer_name"] = df["something"].fillna("unknown customer")

        # Fill missing emails
        df["customer_email"] = df["customer_email"].fillna("missing@email.com")

        # Fill missing order status
        df["order_status"] = df["order_status"].fillna("pending")

        # Convert date column
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        # if success then write file in processed folder
        # Create output directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save processed file
        df.to_csv(output_file, index=False)

        print(f"Processed file saved to {output_file}")
        
        return output_file

    except Exception as e:

        os.makedirs(os.path.dirname(failed_file), exist_ok=True)

        shutil.copy(input_file, failed_file)

        print(f"Saved failed file to {failed_file}")
        print(f"Transformation failed: {e}")

        raise

    
