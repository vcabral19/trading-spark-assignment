from typing import Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from src.app.pivot import pivot
from src.tests.utils_test import assert_dataframes_equal


def test_pivot_functionality_with_default_data(spark: SparkSession, input_data: Tuple[DataFrame]):
    expected_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("bid", DoubleType()),
            StructField("ask", DoubleType()),
            StructField("price", DoubleType()),
            StructField("quantity", DoubleType()),
            StructField("10_bid", DoubleType()),
            StructField("10_ask", DoubleType()),
            StructField("10_price", DoubleType()),
            StructField("10_quantity", DoubleType()),
            StructField("20_bid", DoubleType()),
            StructField("20_ask", DoubleType()),
            StructField("20_price", DoubleType()),
            StructField("20_quantity", DoubleType()),
        ]
    )

    expected_data = [
        Row(
            **{
                "id": 10,
                "timestamp": 1546300799000,
                "bid": 37.5,
                "ask": 37.51,
                "price": None,
                "quantity": None,
                "10_bid": 37.5,
                "10_ask": 37.51,
                "10_price": None,
                "10_quantity": None,
                "20_bid": None,
                "20_ask": None,
                "20_price": None,
                "20_quantity": None,
            }
        ),
        Row(
            **{
                "id": 10,
                "timestamp": 1546300800000,
                "bid": None,
                "ask": None,
                "price": 37.5,
                "quantity": 100.0,
                "10_bid": 37.5,
                "10_ask": 37.51,
                "10_price": 37.5,
                "10_quantity": 100.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": None,
                "20_quantity": None,
            }
        ),
        Row(
            **{
                "id": 10,
                "timestamp": 1546300801000,
                "bid": None,
                "ask": None,
                "price": 37.51,
                "quantity": 100.0,
                "10_bid": 37.5,
                "10_ask": 37.51,
                "10_price": 37.51,
                "10_quantity": 100.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": None,
                "20_quantity": None,
            }
        ),
        Row(
            **{
                "id": 10,
                "timestamp": 1546300802000,
                "bid": 37.51,
                "ask": 37.52,
                "price": None,
                "quantity": None,
                "10_bid": 37.51,
                "10_ask": 37.52,
                "10_price": 37.51,
                "10_quantity": 100.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": None,
                "20_quantity": None,
            }
        ),
        Row(
            **{
                "id": 20,
                "timestamp": 1546300804000,
                "bid": None,
                "ask": None,
                "price": 12.67,
                "quantity": 300.0,
                "10_bid": 37.51,
                "10_ask": 37.52,
                "10_price": 37.51,
                "10_quantity": 100.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": 12.67,
                "20_quantity": 300.0,
            }
        ),
        Row(
            **{
                "id": 10,
                "timestamp": 1546300806000,
                "bid": 37.5,
                "ask": 37.51,
                "price": None,
                "quantity": None,
                "10_bid": 37.5,
                "10_ask": 37.51,
                "10_price": 37.51,
                "10_quantity": 100.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": 12.67,
                "20_quantity": 300.0,
            }
        ),
        Row(
            **{
                "id": 10,
                "timestamp": 1546300807000,
                "bid": None,
                "ask": None,
                "price": 37.5,
                "quantity": 200.0,
                "10_bid": 37.5,
                "10_ask": 37.51,
                "10_price": 37.5,
                "10_quantity": 200.0,
                "20_bid": None,
                "20_ask": None,
                "20_price": 12.67,
                "20_quantity": 300.0,
            }
        ),
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    trades_df, prices_df = input_data
    result_df = pivot(trades_df, prices_df, spark)

    assert_dataframes_equal(result_df, expected_df)


def test_pivot_functionality_with_more_ids(spark: SparkSession, multiple_id_input_data: Tuple[DataFrame]):
    expected_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("bid", DoubleType()),
            StructField("ask", DoubleType()),
            StructField("price", DoubleType()),
            StructField("quantity", DoubleType()),
            StructField("1_bid", DoubleType()),
            StructField("1_ask", DoubleType()),
            StructField("1_price", DoubleType()),
            StructField("1_quantity", DoubleType()),
            StructField("2_bid", DoubleType()),
            StructField("2_ask", DoubleType()),
            StructField("2_price", DoubleType()),
            StructField("2_quantity", DoubleType()),
            StructField("3_bid", DoubleType()),
            StructField("3_ask", DoubleType()),
            StructField("3_price", DoubleType()),
            StructField("3_quantity", DoubleType()),
        ]
    )

    expected_data = [
        Row(
            **{
                "id": 1,
                "timestamp": 1,
                "bid": 9.95,
                "ask": 10.01,
                "price": None,
                "quantity": None,
                "1_bid": 9.95,
                "1_ask": 10.01,
                "1_price": None,
                "1_quantity": None,
                "2_bid": None,
                "2_ask": None,
                "2_price": None,
                "2_quantity": None,
                "3_bid": None,
                "3_ask": None,
                "3_price": None,
                "3_quantity": None,
            }
        ),
        Row(
            **{
                "id": 1,
                "timestamp": 2,
                "bid": None,
                "ask": None,
                "price": 37.52,
                "quantity": 600.0,
                "1_bid": 9.95,
                "1_ask": 10.01,
                "1_price": 37.52,
                "1_quantity": 600.0,
                "2_bid": None,
                "2_ask": None,
                "2_price": None,
                "2_quantity": None,
                "3_bid": None,
                "3_ask": None,
                "3_price": None,
                "3_quantity": None,
            }
        ),
        Row(
            **{
                "id": 2,
                "timestamp": 3,
                "bid": 14.98,
                "ask": 15.04,
                "price": None,
                "quantity": None,
                "1_bid": 9.95,
                "1_ask": 10.01,
                "1_price": 37.52,
                "1_quantity": 600.0,
                "2_bid": 14.98,
                "2_ask": 15.04,
                "2_price": None,
                "2_quantity": None,
                "3_bid": None,
                "3_ask": None,
                "3_price": None,
                "3_quantity": None,
            }
        ),
        Row(
            **{
                "id": 3,
                "timestamp": 4,
                "bid": 37.48,
                "ask": 37.53,
                "price": None,
                "quantity": None,
                "1_bid": 9.95,
                "1_ask": 10.01,
                "1_price": 37.52,
                "1_quantity": 600.0,
                "2_bid": 14.98,
                "2_ask": 15.04,
                "2_price": None,
                "2_quantity": None,
                "3_bid": 37.48,
                "3_ask": 37.53,
                "3_price": None,
                "3_quantity": None,
            }
        ),
        Row(
            **{
                "id": 2,
                "timestamp": 5,
                "bid": None,
                "ask": None,
                "price": 20.0,
                "quantity": 100.0,
                "1_bid": 9.95,
                "1_ask": 10.01,
                "1_price": 37.52,
                "1_quantity": 600.0,
                "2_bid": 14.98,
                "2_ask": 15.04,
                "2_price": 20.0,
                "2_quantity": 100.0,
                "3_bid": 37.48,
                "3_ask": 37.53,
                "3_price": None,
                "3_quantity": None,
            }
        ),
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    trades_df, prices_df = multiple_id_input_data
    result_df = pivot(trades_df, prices_df, spark)

    assert_dataframes_equal(result_df, expected_df)
