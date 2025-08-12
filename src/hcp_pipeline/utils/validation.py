from pyspark.sql import DataFrame

def expect_non_null(df: DataFrame, column: str) -> bool:
    nulls = df.filter(df[column].isNull()).count()
    return nulls == 0
