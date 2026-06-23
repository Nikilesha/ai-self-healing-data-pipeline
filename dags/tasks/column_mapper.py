import logging

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "cust_id": "customer_id",
    "full_name": "customer_name",
    "email": "customer_email",
    "email_address": "customer_email",
    "qty": "quantity",
    "amount": "total_amount",
    "total_price": "total_amount",
    "price": "unit_price",
    "category": "product_category",
    "status": "order_status",
    "address": "shipping_address",
    "zipcode": "postal_code"
}


def auto_map_columns(df):

    rename_dict = {}

    for col in df.columns:

        col_lower = col.lower().strip()

        if col_lower in COLUMN_MAPPING:

            rename_dict[col] = COLUMN_MAPPING[col_lower]

            logger.warning(
                f"Auto Mapping Applied: "
                f"{col} -> {COLUMN_MAPPING[col_lower]}"
            )
            

    if rename_dict:
        df = df.rename(columns=rename_dict)
    df = df.loc[:, ~df.columns.duplicated()]

    return df