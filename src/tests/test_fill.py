from typing import Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from src.app.fill import fill
from src.tests.utils_test import assert_dataframes_equal


def test_fill_forward_functionality(spark: SparkSession, input_data: Tuple[DataFrame]):
    expected_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("bid", DoubleType()),
            StructField("ask", DoubleType()),
            StructField("price", DoubleType()),
            StructField("quantity", DoubleType()),
        ]
    )
    expected_data = [
        Row(id=10, timestamp=1546300799000, bid=37.5, ask=37.51, price=None, quantity=None),
        Row(id=10, timestamp=1546300800000, bid=37.5, ask=37.51, price=37.5, quantity=100.0),
        Row(id=10, timestamp=1546300801000, bid=37.5, ask=37.51, price=37.51, quantity=100.0),
        Row(id=10, timestamp=1546300802000, bid=37.51, ask=37.52, price=37.51, quantity=100.0),
        Row(id=10, timestamp=1546300806000, bid=37.5, ask=37.51, price=37.51, quantity=100.0),
        Row(id=10, timestamp=1546300807000, bid=37.5, ask=37.51, price=37.5, quantity=200.0),
        Row(id=20, timestamp=1546300804000, bid=None, ask=None, price=12.67, quantity=300.0),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    trades_df, prices_df = input_data
    filled_df = fill(trades_df, prices_df)

    print("filled_df: ")
    filled_df.show()
    print("expected_df: ")
    expected_df.show()
    assert_dataframes_equal(filled_df, expected_df)
