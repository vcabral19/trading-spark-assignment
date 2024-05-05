from typing import List

from pyspark.sql import DataFrame


def assert_dataframes_equal(result_df: DataFrame, expected_df: DataFrame, cols_sorted: List[str] = ["id", "timestamp"]):
    sorted_df1 = result_df.select(sorted(result_df.columns)).orderBy(cols_sorted)
    sorted_df2 = expected_df.select(sorted(expected_df.columns)).orderBy(cols_sorted)
    assert sorted_df1.collect() == sorted_df2.collect(), "DataFrames are not equal!"
