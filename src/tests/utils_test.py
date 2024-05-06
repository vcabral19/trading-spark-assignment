from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import round as spark_round


def assert_dataframes_equal(result_df: DataFrame, expected_df: DataFrame, cols_sorted: List[str] = ["id", "timestamp"]):
    sorted_df1 = result_df.select(sorted(result_df.columns)).orderBy(cols_sorted)
    sorted_df2 = expected_df.select(sorted(expected_df.columns)).orderBy(cols_sorted)
    print("sorted_result_df: ")
    sorted_df1.show()
    print("sorted_expected_df: ")
    sorted_df2.show()
    assert sorted_df1.collect() == sorted_df2.collect()


def round_all_columns(df: DataFrame) -> DataFrame:
    for column_name, column_type in df.dtypes:
        if column_type in ("double", "float"):
            df = df.withColumn(column_name, spark_round(col(column_name), 1))
    return df
