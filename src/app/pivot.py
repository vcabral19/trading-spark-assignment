import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import TimestampType


def pivot(trade_events_df: DataFrame, price_events_df: DataFrame) -> DataFrame:
    """
    Pivot and fill the columns on the event id so that each row contains a
    column for each id + column combination where the value is the most recent
    non-null value for that id. For example, given the above input tables the
    expected output is:

    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | id|    timestamp|  bid|  ask|price|quantity|10_bid|10_ask|10_price|10_quantity|20_bid|20_ask|20_price|20_quantity|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | 10|1546300799000| 37.5|37.51| null|    null|  37.5| 37.51|    null|       null|  null|  null|    null|       null|
    | 10|1546300800000| null| null| 37.5|   100.0|  37.5| 37.51|    37.5|      100.0|  null|  null|    null|       null|
    | 10|1546300801000| null| null|37.51|   100.0|  37.5| 37.51|   37.51|      100.0|  null|  null|    null|       null|
    | 10|1546300802000|37.51|37.52| null|    null| 37.51| 37.52|   37.51|      100.0|  null|  null|    null|       null|
    | 20|1546300804000| null| null|12.67|   300.0| 37.51| 37.52|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300806000| 37.5|37.51| null|    null|  37.5| 37.51|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300807000| null| null| 37.5|   200.0|  37.5| 37.51|    37.5|      200.0|  null|  null|   12.67|      300.0|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and pivoted columns.
    """
    combined_df = trade_events_df.unionByName(price_events_df, allowMissingColumns=True)

    partition_by_day = F.date_trunc("day", F.col("timestamp").cast(TimestampType()))
    combined_df = combined_df.repartition(partition_by_day)
    combined_df.cache()

    distinct_ids = [row.id for row in combined_df.select("id").distinct().collect()]

    # partition by day as it seems it might the most relevant interval that still scales for several days
    window_spec = (
        Window.partitionBy(partition_by_day)
        .orderBy(F.col("timestamp"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    select_expressions = ["id", "timestamp", "bid", "ask", "price", "quantity"]
    for id_val in distinct_ids:
        for measure in ["bid", "ask", "price", "quantity"]:
            col_name = f"{id_val}_{measure}"
            expr = (
                F.last(F.when(F.col("id") == id_val, F.col(measure)).otherwise(None), ignorenulls=True)
                .over(window_spec)
                .alias(col_name)
            )
            select_expressions.append(expr)

    result_df = combined_df.select(*select_expressions)

    combined_df.unpersist()

    return result_df
