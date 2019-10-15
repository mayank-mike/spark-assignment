from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    session = SparkSession.builder.appName("HousePriceSolution").master("local[*]").getOrCreate()

    stockPrices = session.read \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("stock_prices.csv")

    stockPrices.createOrReplaceTempView("stockPricesView")

    average = session.sql("SELECT date, AVG((close-open)*volume) AS AveragePrice from stockPricesView GROUP BY date")
    average.select("date", "AveragePrice").coalesce(1).write.save("averagePrice.csv", format="csv", header="true")

    mostFrequently = session.sql(
        "SELECT ticker, avg(close*volume) as avgprices from stockPricesView group by ticker order by avgprices desc "
        "limit 1")
    mostFrequently.show()
