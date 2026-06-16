import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def task_load_data():

    csv_path = os.path.join(
        os.getenv("VALIDATED_DATA_PATH"),
        "orders_validated.csv"
    )

    df = pd.read_csv(csv_path)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()

    inserted_rows = 0
    skipped_rows = 0

    try:
        for _, row in df.iterrows():

            order_date = row["order_date"]

            # handle NaN / empty
            if pd.isna(order_date) or order_date == "":
                order_date = None
            else:
                order_date = pd.to_datetime(order_date, errors="coerce")
                order_date = None if pd.isna(order_date) else order_date.to_pydatetime()

            cursor.execute("""
                INSERT INTO orders (
                    order_id,
                    customer_name,
                    customer_email,
                    order_date,
                    quantity,
                    total_amount
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                str(row["order_id"]),
                row["customer_name"],
                row["customer_email"],
                order_date,
                int(row["quantity"]),
                float(row["total_amount"])
            ))

            if cursor.rowcount == 1:
                inserted_rows += 1
            else:
                skipped_rows += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()

    print(f"Inserted: {inserted_rows}, Skipped: {skipped_rows}")

    return {
        "rows_inserted": inserted_rows,
        "rows_skipped": skipped_rows
    }