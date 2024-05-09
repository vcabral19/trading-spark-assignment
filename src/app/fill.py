import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType, TimestampType


def fill(trade_events_df: DataFrame, price_events_df: DataFrame) -> DataFrame:
    """
    Combine the sets of events and fill forward the value columns so that each
    row has the most recent non-null value for the corresponding id. For
    example, given the above input tables the expected output is:

    +---+-------------+-----+-----+-----+--------+
    | id|    timestamp|  bid|  ask|price|quantity|
    +---+-------------+-----+-----+-----+--------+
    | 10|1546300799000| 37.5|37.51| null|    null|
    | 10|1546300800000| 37.5|37.51| 37.5|   100.0|
    | 10|1546300801000| 37.5|37.51|37.51|   100.0|
    | 10|1546300802000|37.51|37.52|37.51|   100.0|
    | 20|1546300804000| null| null|12.67|   300.0|
    | 10|1546300806000| 37.5|37.51|37.51|   100.0|
    | 10|1546300807000| 37.5|37.51| 37.5|   200.0|
    +---+-------------+-----+-----+-----+--------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and filled.
    """
    trade_events_df = trade_events_df.withColumn("bid", F.lit(None).cast(DoubleType())).withColumn(
        "ask", F.lit(None).cast(DoubleType())
    )
    price_events_df = price_events_df.withColumn("price", F.lit(None).cast(DoubleType())).withColumn(
        "quantity", F.lit(None).cast(DoubleType())
    )

    combined_events_df = trade_events_df.unionByName(
        price_events_df
    )  # allowMissingColumns=True would make adding the columns obsolete

    partition_by_day = F.date_trunc("day", F.col("timestamp").cast(TimestampType()))
    combined_events_df = combined_events_df.repartition(partition_by_day)
    combined_events_df.cache()

    window_spec = (
        Window.partitionBy(F.col("id"))
        .orderBy(F.col("timestamp"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    value_columns = ["bid", "ask", "price", "quantity"]
    select_expressions = ["id", "timestamp"] + [
        F.last(F.col(column_name), ignorenulls=True).over(window_spec).alias(column_name)
        for column_name in value_columns
    ]

    result_df = combined_events_df.select(*select_expressions).orderBy(F.col("id"), F.col("timestamp"))

    combined_events_df.unpersist()
    return result_df
