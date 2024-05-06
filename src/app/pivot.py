import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import broadcast, col, last, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import when

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
    # Combine the data into a single DataFrame
    combined_df = trade_events_df.unionByName(price_events_df, allowMissingColumns=True)

    # Get distinct IDs from the DataFrame
    distinct_ids = [row["id"] for row in combined_df.select("id").distinct().collect()]

    # Define a global window specification ordered by timestamp
    window_spec = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Create columns for each ID and measure combination without affecting the original columns
    for id_val in distinct_ids:
        for measure in ["bid", "ask", "price", "quantity"]:
            col_name = f"{id_val}_{measure}"
            combined_df = combined_df.withColumn(
                col_name, last(when(col("id") == id_val, col(measure)), ignorenulls=True).over(window_spec)
            )

    # Select the required columns to structure the DataFrame as specified
    select_exprs = ["id", "timestamp", "bid", "ask", "price", "quantity"] + [
        f"{id}_{metric}" for id in distinct_ids for metric in ["bid", "ask", "price", "quantity"]
    ]

    return combined_df.select(select_exprs)
