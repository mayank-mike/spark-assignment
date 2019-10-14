from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def isNotHeader(line: str):
    return not (line.startswith("date") and "volume" in line)


if __name__ == "__main__":
    session = SparkSession.builder.appName("HousePriceSolution").master("local[*]").getOrCreate()

    stockPrices = session.read \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("stock_prices.csv")

    stockPrices.groupBy("date").avg("open").show(1)


