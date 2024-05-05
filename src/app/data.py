from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType


def load_trades(spark: SparkSession) -> DataFrame:
    data = [
        (10, 1546300800000, 37.50, 100.000),
        (10, 1546300801000, 37.51, 100.000),
        (20, 1546300804000, 12.67, 300.000),
        (10, 1546300807000, 37.50, 200.000),
    ]
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("price", DoubleType()),
            StructField("quantity", DoubleType()),
        ]
    )

    return spark.createDataFrame(data, schema)


def load_prices(spark: SparkSession) -> DataFrame:
    data = [
        (10, 1546300799000, 37.50, 37.51),
        (10, 1546300802000, 37.51, 37.52),
        (10, 1546300806000, 37.50, 37.51),
    ]
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("timestamp", LongType()),
            StructField("bid", DoubleType()),
            StructField("ask", DoubleType()),
        ]
    )

    return spark.createDataFrame(data, schema)
