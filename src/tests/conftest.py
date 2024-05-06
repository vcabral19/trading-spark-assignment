from typing import Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from src.app.data import load_prices, load_trades


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest-pyspark-local-testing").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def input_data(spark: SparkSession) -> Tuple[DataFrame]:
    return load_trades(spark), load_prices(spark)


@pytest.fixture(scope="function")
def multiple_id_input_data(spark: SparkSession) -> Tuple[DataFrame]:
    trade_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("price", DoubleType()),
            StructField("quantity", DoubleType()),
        ]
    )

    price_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("bid", DoubleType()),
            StructField("ask", DoubleType()),
        ]
    )

    trades_data = [
        Row(id=1, timestamp=2, price=37.52, quantity=600.0),
        Row(id=2, timestamp=5, price=20.0, quantity=100.0),
    ]

    prices_data = [
        Row(id=1, timestamp=1, bid=9.95, ask=10.01),
        Row(id=2, timestamp=3, bid=14.98, ask=15.04),
        Row(id=3, timestamp=4, bid=37.48, ask=37.53),
    ]

    trades_df = spark.createDataFrame(trades_data, schema=trade_schema)
    prices_df = spark.createDataFrame(prices_data, schema=price_schema)
    return trades_df, prices_df
