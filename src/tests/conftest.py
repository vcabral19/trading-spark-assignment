from typing import Tuple

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.app.data import load_prices, load_trades


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pytest-pyspark-local-testing").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def input_data(spark: SparkSession) -> Tuple[DataFrame]:
    return load_trades(spark), load_prices(spark)
