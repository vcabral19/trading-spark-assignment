from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, last, when

from src.app.fill import fill


def pivot(trade_events_df: DataFrame, price_events_df: DataFrame, spark: SparkSession) -> DataFrame:
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
    filled_df = fill(trade_events_df, price_events_df)
    filled_df.cache()

    distinct_ids_list = filled_df.select("id").distinct().rdd.flatMap(lambda x: x).collect()
    broadcast_ids = spark.sparkContext.broadcast(distinct_ids_list)

    window_spec = (
        Window.partitionBy("id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    for id_val in broadcast_ids.value:
        for measure in ["bid", "ask", "price", "quantity"]:
            col_name = f"{id_val}_{measure}"
            filled_df = filled_df.withColumn(col_name, when(col("id") == id_val, col(measure)))

            filled_df = filled_df.withColumn(col_name, last(col(col_name), True).over(window_spec))

    column_order = ["id", "timestamp", "bid", "ask", "price", "quantity"] + [
        f"{id}_{measure}" for id in broadcast_ids.value for measure in ["bid", "ask", "price", "quantity"]
    ]
    final_df = filled_df.select(column_order)
    return final_df
