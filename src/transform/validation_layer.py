import pandas as pd
import logging
log = logging.getLogger(__name__)


def validate_customers(df: pd.DataFrame) -> None:
    assert df["customer_id"].notna().all(), "Null customer_ids found"
    assert df["customer_id"].nunique() == len(df), "Duplicate customer_ids found"
    assert df["customer_unique_id"].notna().all(), "Null customer_unique_ids found"
    assert df["customer_zip_code_prefix"].notna().all(), "Null zip codes in customers"
    assert df["customer_state"].notna().all(), "Null states in customers"
    log.info(f"customers: {len(df)} rows OK")


def validate_orders(df: pd.DataFrame) -> None:
    assert df["order_id"].notna().all(), "Null order_ids found"
    assert df["order_id"].nunique() == len(df), "Duplicate order_ids found"
    assert df["customer_id"].notna().all(), "Null customer_ids in orders"
    assert df["order_purchase_timestamp"].notna().all(), "Null purchase timestamps"

    valid_statuses = {
        "delivered", "shipped", "canceled", "invoiced",
        "processing", "approved", "unavailable", "created",
        "delivered_no_date"  # ← add this
    }

    invalid = df[~df["order_status"].isin(valid_statuses)]
    assert len(invalid) == 0, f"Unexpected order statuses: {invalid['order_status'].unique()}"

    delivered = df[df["order_status"] == "delivered"]
    assert (
        delivered["order_delivered_customer_date"].notna().all()
    ), "Delivered orders with null delivery date"
    log.info(f"orders: {len(df)} rows OK")


def validate_order_items(df: pd.DataFrame) -> None:
    assert df["order_id"].notna().all(), "Null order_ids in order_items"
    assert df["product_id"].notna().all(), "Null product_ids in order_items"
    assert df["seller_id"].notna().all(), "Null seller_ids in order_items"
    assert (df["price"] >= 0).all(), "Negative prices in order_items"
    assert (df["freight_value"] >= 0).all(), "Negative freight values in order_items"
    assert (df["order_item_id"] >= 1).all(), "order_item_id must be >= 1"
    log.info(f"order_items: {len(df)} rows OK")


def validate_payments(df: pd.DataFrame) -> None:
    assert df["order_id"].notna().all(), "Null order_ids in payments"
    assert (df["payment_value"] >= 0).all(), "Negative payment values"

    assert (df["payment_installments"] >= 0).all(), "Negative payment_installments found"

    valid_types = {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
    invalid = df[~df["payment_type"].isin(valid_types)]
    assert len(invalid) == 0, f"Unexpected payment types: {invalid['payment_type'].unique()}"
    log.info(f"payments: {len(df)} rows OK")


def validate_reviews(df: pd.DataFrame) -> None:
    assert df["review_id"].notna().all(), "Null review_ids found"
    assert df["order_id"].notna().all(), "Null order_ids in reviews"
    assert df["review_score"].between(1, 5).all(), "review_score must be between 1 and 5"
    log.info(f"reviews: {len(df)} rows OK")


def validate_products(df: pd.DataFrame) -> None:
    assert df["product_id"].notna().all(), "Null product_ids found"
    assert df["product_id"].nunique() == len(df), "Duplicate product_ids found"

    numeric_cols = [
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    for col in numeric_cols:
        if col in df.columns:
            assert (df[col].dropna() >= 0).all(), f"Negative values in {col}"
    log.info(f"products: {len(df)} rows OK")


def validate_sellers(df: pd.DataFrame) -> None:
    assert df["seller_id"].notna().all(), "Null seller_ids found"
    assert df["seller_id"].nunique() == len(df), "Duplicate seller_ids found"
    assert df["seller_zip_code_prefix"].notna().all(), "Null zip codes in sellers"
    assert df["seller_state"].notna().all(), "Null states in sellers"
    log.info(f"sellers: {len(df)} rows OK")


def validate_geolocation(df: pd.DataFrame) -> None:
    assert df["geolocation_zip_code_prefix"].notna().all(), "Null zip codes in geolocation"
    assert df["geolocation_lat"].between(-90, 90).all(), "Invalid latitudes"
    assert df["geolocation_lng"].between(-180, 180).all(), "Invalid longitudes"
    assert df["geolocation_state"].notna().all(), "Null states in geolocation"
    log.info(f"geolocation: {len(df)} rows OK")


def validate_category_translation(df: pd.DataFrame) -> None:
    assert df["product_category_name"].notna().all(), "Null category names found"
    assert (
        df["product_category_name"].nunique() == len(df)
    ), "Duplicate category names in translation table"
    assert df["product_category_name_english"].notna().all(), "Null english translations found"
    log.info(f"category_translation: {len(df)} rows OK")


def validate_all_tables(tables: dict[str, pd.DataFrame]) -> None:
    log.info("Starting validation...")
    validate_customers(tables["customers"])
    validate_orders(tables["orders"])
    validate_order_items(tables["order_items"])
    validate_payments(tables["payments"])
    validate_reviews(tables["reviews"])
    validate_products(tables["products"])
    validate_sellers(tables["sellers"])
    validate_geolocation(tables["geolocation"])
    validate_category_translation(tables["category_translation"])
    log.info("All validations passed.")