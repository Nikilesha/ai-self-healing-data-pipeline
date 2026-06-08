import pandas as pd


def task_validate_data(file_path):

    df = pd.read_json(file_path)

    report = {
        "total_records": len(df),
        "missing_customer_name": df["CUSTOMER_NAME"].isnull().sum(),
        "missing_email": df["customer_email"].isnull().sum(),
        "missing_order_date": df["ORDER_DATE"].isnull().sum(),
        "duplicate_orders": df["ORDER_ID"].duplicated().sum(),
        "negative_quantity": (df["QUANTITY"] < 0).sum(),
        "negative_amount": (df["TOTAL_AMOUNT"] < 0).sum()
    }

    print("\nValidation Report")
    print(report)

    return report