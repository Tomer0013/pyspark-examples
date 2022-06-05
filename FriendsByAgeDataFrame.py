"""This script matches FriendsByAgeDataset.scala"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs


def main() -> None:
    spark = SparkSession.builder.appName("FriendsByAgeDataFrame").master("local[*]").getOrCreate()
    ds = spark.read.option("header", True).option("inferSchema", True).csv("./data/fakefriends.csv")
    friends_by_age = ds.select("age", "friends")

    # From friendsByAge we group by "age" and then compute average and sort by age col
    friends_by_age.groupby("age").avg("friends").sort("age").show()

    # Nicer format
    friends_by_age.groupby("age").agg(funcs.round(funcs.avg("friends"), scale=2).
                                      alias("friends_avg")).sort("age").show()

    spark.stop()


if __name__ == "__main__":
    main()
