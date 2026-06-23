import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()


def task_load_data():

    csv_path = os.path.join(os.getenv("VALIDATED_DATA_PATH"), "orders_validated.csv")
    file_name = os.path.basename(csv_path)

    
    try:
        # ---------- establishing connection ----------#
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

        cursor = conn.cursor()

        

        # ---------- table creation ----------#
        # TABLE ORDERS
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(255),
            customer_email VARCHAR(255),
            order_date TIMESTAMP,
            quantity INTEGER,
            total_amount NUMERIC 
        )
        """)
        # TABLE PROCESSED FILES
        cursor.execute("""
            create table if not exists processed_files(
                file_name varchar(255) primary key,
                processed_at timestamp           
            )
    """)
        conn.commit()

        cursor.execute("""
            SELECT 1
            FROM processed_files
            WHERE file_name = %s
        """, (file_name,))

        if cursor.fetchone():
            logger.info(f"{file_name} already processed")
            cursor.close()
            conn.close()

            return {
                "processed_rows": 0
            }
        
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error("Database Connection Failed")
        logger.error(e)
        raise

    processed_rows = 0

    try:
        for _, row in df.iterrows():

            order_date = row["order_date"]

            # handle NaN / empty
            if pd.isna(order_date) or order_date == "":
                order_date = None
            else:
                order_date = pd.to_datetime(order_date, errors="coerce")
                order_date = None if pd.isna(order_date) else order_date.to_pydatetime()

            cursor.execute(
                """
                INSERT INTO orders (
                    order_id,
                    customer_name,
                    customer_email,
                    order_date,
                    quantity,
                    total_amount
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id)
                DO UPDATE SET 
                customer_name = EXCLUDED.customer_name,
                customer_email = EXCLUDED.customer_email,
                order_date = EXCLUDED.order_date,
                quantity = EXCLUDED.quantity,
                total_amount = EXCLUDED.total_amount;
            """,
                (
                    str(row["order_id"]),
                    row["customer_name"],
                    row["customer_email"],
                    order_date,
                    int(row["quantity"]),
                    float(row["total_amount"]),
                ),
            )

            processed_rows += 1

        cursor.execute(
            """
            INSERT INTO processed_files(
                file_name,
                processed_at
            )
            VALUES (%s, NOW())
            ON CONFLICT DO NOTHING
        """,
            (file_name,),
        )

        conn.commit()

        logger.info(f"{file_name} marked as processed")

    except Exception as e:
        conn.rollback()
        logger.error("Error writing to database")
        logger.error(e)
        raise e

    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed")

    logger.info(f"Processed Rows: {processed_rows}")

    return {
        "processed_rows": processed_rows
    }
