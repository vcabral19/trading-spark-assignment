from pyspark.sql import SparkSession

from src.app.data import load_prices, load_trades
from src.app.fill import fill
from src.app.pivot import pivot


def main():
    spark = SparkSession.builder.appName("Spark Assignement 2.0.0").getOrCreate()

    trades = load_trades(spark)
    trades.show()

    prices = load_prices(spark)
    prices.show()

    print("Printing forward fill DataFrame: ")
    #fill(trades, prices).explain()
    fill(trades, prices).show()

    print("Printing pivot DataFrame: ")
    #pivot(trades, prices).explain()
    pivot(trades, prices).show()


if __name__ == "__main__":
    main()
