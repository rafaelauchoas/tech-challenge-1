import pandas as pd

def prepare_dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()
    df = df.rename(columns={
        "customer_zip_code_prefix": "zip_code_prefix",
        "customer_city": "city",
        "customer_state": "state",
    })
    return df

def prepare_dim_products(products: pd.DataFrame, category_translation: pd.DataFrame) -> pd.DataFrame:
    df = products.copy()

    df = df.merge(
        category_translation,
        on="product_category_name",
        how="left"
    )

    df["product_category_name"] = df["product_category_name"].fillna("unknown")
    df["product_category_name_english"] = df["product_category_name_english"].fillna("unknown")

    return df


def prepare_dim_sellers(sellers: pd.DataFrame) -> pd.DataFrame:
    df = sellers.copy()

    df = df.rename(columns={
        "seller_zip_code_prefix": "zip_code_prefix",
        "seller_city": "city",
        "seller_state": "state",
    })

    return df


def prepare_dim_geolocation(geolocation: pd.DataFrame) -> pd.DataFrame:
    df = geolocation.copy()
    df = df.groupby("geolocation_zip_code_prefix").agg(
        city=("geolocation_city", "first"),
        state=("geolocation_state", "first"),
        latitude=("geolocation_lat", "mean"),
        longitude=("geolocation_lng", "mean"),
    ).reset_index().rename(columns={"geolocation_zip_code_prefix": "zip_code_prefix"})
    return df

def prepare_fact_orders(orders: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()

    df["delivery_time_days"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    df["approval_time_days"] = (
        df["order_approved_at"] - df["order_purchase_timestamp"]
    ).dt.days

    df["delay_days"] = (
        df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
    ).dt.days

    # Delayed => Pedido entregue E chegou atrasado.
    # Pedidos não entregues (NaN delay_days) continuam como NaN.
    df["is_delayed"] = df["delay_days"].apply(
        lambda x: 1 if pd.notnull(x) and x > 0 else (0 if pd.notnull(x) else pd.NA)
    )

    return df

def prepare_fact_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    df = order_items.copy()
    df["total_item_value"] = df["price"] + df["freight_value"]
    return df

def prepare_fact_payments(payments: pd.DataFrame) -> pd.DataFrame:
    df = payments.copy()

    # Total value paid per order (across all payment installments)
    df["total_order_payment"] = df.groupby("order_id")["payment_value"].transform("sum")

    return df

def prepare_fact_reviews(reviews: pd.DataFrame) -> pd.DataFrame:
    df = reviews.copy()

    # Flag low-score reviews for easy filtering in Power BI
    df["is_negative_review"] = df["review_score"].apply(
        lambda x: 1 if pd.notnull(x) and x <= 2 else 0
    )

    return df

def prepare_powerbi_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dim_customers = prepare_dim_customers(tables["customers"])
    dim_products = prepare_dim_products(
        tables["products"],
        tables["category_translation"]
    )
    dim_sellers = prepare_dim_sellers(tables["sellers"])
    dim_geolocation = prepare_dim_geolocation(tables["geolocation"])

    fact_orders = prepare_fact_orders(tables["orders"])
    fact_order_items = prepare_fact_order_items(tables["order_items"])
    fact_payments = prepare_fact_payments(tables["payments"])
    fact_reviews = prepare_fact_reviews(tables["reviews"])
    dim_date = prepare_dim_date(tables["orders"])

    return {
        "dim_date": dim_date,
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_sellers": dim_sellers,
        "dim_geolocation": dim_geolocation,
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "fact_payments": fact_payments,
        "fact_reviews": fact_reviews,
    }

def prepare_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    min_date = orders["order_purchase_timestamp"].min()
    max_date = orders["order_purchase_timestamp"].max()
    dates = pd.date_range(min_date, max_date, freq="D")
    return pd.DataFrame({
        "date": dates,
        "year": dates.year,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "quarter": dates.quarter,
        "week": dates.isocalendar().week.astype(int),
        "day_of_week": dates.day_name(),
        "is_weekend": dates.dayofweek >= 5,
    })