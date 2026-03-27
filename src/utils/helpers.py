import pandas as pd

def read_csv_safe(path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8", sep=",", on_bad_lines="warn")

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
    )
    return df