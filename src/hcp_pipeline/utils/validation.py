"""Simple validation helpers for pandas DataFrames."""

import pandas as pd


def expect_non_null(df: pd.DataFrame, column: str) -> bool:
    """Return True if the specified column contains no null values."""

    return df[column].isnull().sum() == 0
