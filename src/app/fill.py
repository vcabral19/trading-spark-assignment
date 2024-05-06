from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, last, lit
from pyspark.sql.types import DoubleType


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
    trade_events_df = trade_events_df.withColumn("bid", lit(None).cast(DoubleType())).withColumn(
        "ask", lit(None).cast(DoubleType())
    )
    price_events_df = price_events_df.withColumn("price", lit(None).cast(DoubleType())).withColumn(
        "quantity", lit(None).cast(DoubleType())
    )

    combined_events_df = trade_events_df.unionByName(
        price_events_df
    )  # allowMissingColumns=True would make adding the columns obsolete

    combined_events_df.cache()

    window_spec = (
        Window.partitionBy("id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    # adding rowsBetween just for the sake of clarity as this is the standard behaviour

    # ff for every column
    for column_name in ["bid", "ask", "price", "quantity"]:
        combined_events_df = combined_events_df.withColumn(
            column_name, last(col(column_name), ignorenulls=True).over(window_spec)
        )

    return combined_events_df.orderBy("id", "timestamp")
