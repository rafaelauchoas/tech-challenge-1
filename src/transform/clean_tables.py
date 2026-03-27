import pandas as pd

from src.config import RAW_DIR


def load_raw_tables() -> dict[str, pd.DataFrame]:
    tables = {
        "customers": pd.read_csv(RAW_DIR / "olist_customers_dataset.csv"),
        "orders": pd.read_csv(RAW_DIR / "olist_orders_dataset.csv"),
        "order_items": pd.read_csv(RAW_DIR / "olist_order_items_dataset.csv"),
        "payments": pd.read_csv(RAW_DIR / "olist_order_payments_dataset.csv"),
        "reviews": pd.read_csv(RAW_DIR / "olist_order_reviews_dataset.csv"),
        "products": pd.read_csv(RAW_DIR / "olist_products_dataset.csv"),
        "sellers": pd.read_csv(RAW_DIR / "olist_sellers_dataset.csv"),
        "geolocation": pd.read_csv(RAW_DIR / "olist_geolocation_dataset.csv"),
        "category_translation": pd.read_csv(RAW_DIR / "product_category_name_translation.csv"),
    }
    return tables


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df = df.drop_duplicates()
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    if "shipping_limit_date" in df.columns:
        df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

    return df

def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    if "review_creation_date" in df.columns:
        df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce")

    if "review_answer_timestamp" in df.columns:
        df["review_answer_timestamp"] = pd.to_datetime(df["review_answer_timestamp"], errors="coerce")

    df = df.drop_duplicates()
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df["product_category_name"] = df["product_category_name"].fillna("unknown")
    return df


def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def clean_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def clean_category_translation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def clean_all_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    cleaned = {
        "customers": clean_customers(tables["customers"]),
        "orders": clean_orders(tables["orders"]),
        "order_items": clean_order_items(tables["order_items"]),
        "payments": clean_payments(tables["payments"]),
        "reviews": clean_reviews(tables["reviews"]),
        "products": clean_products(tables["products"]),
        "sellers": clean_sellers(tables["sellers"]),
        "geolocation": clean_geolocation(tables["geolocation"]),
        "category_translation": clean_category_translation(tables["category_translation"]),
    }
    return cleaned