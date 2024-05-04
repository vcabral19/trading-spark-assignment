from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, last, lit


def fill(trades: DataFrame, prices: DataFrame) -> DataFrame:
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
    # Adjust trades DataFrame to match the prices schema
    trades = trades.withColumn("bid", lit(None).cast("double")).withColumn("ask", lit(None).cast("double"))

    # Adjust prices DataFrame to include a 'quantity' column
    prices = prices.withColumn("price", lit(None).cast("double")).withColumn("quantity", lit(None).cast("double"))

    # Union the adjusted DataFrames
    combined = trades.unionByName(prices)

    # Define window specification for forward fill
    window_spec = Window.partitionBy("id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

    # Apply forward fill using last with ignoreNulls for each column
    for column_name in ["bid", "ask", "price", "quantity"]:
        combined = combined.withColumn(column_name, last(col(column_name), ignoreNulls=True).over(window_spec))

    return combined.orderBy("id", "timestamp")
