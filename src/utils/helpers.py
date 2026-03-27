import pandas as pd


def read_csv_safe(path):
    return pd.read_csv(path)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
    )
    return df