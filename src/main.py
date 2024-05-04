from pyspark.sql import SparkSession

from src.data import load_prices, load_trades
from src.fill import fill
from src.pivot import pivot


def main():
    spark = SparkSession.builder.master("local[*]").appName("Spark Assignement 2.0.0").getOrCreate()

    trades = load_trades(spark)
    trades.show()

    prices = load_prices(spark)
    prices.show()

    print("Printing forward fill DataFrame: ")
    fill(trades, prices).show()

    pivot(trades, prices).show()


if __name__ == "__main__":
    main()
