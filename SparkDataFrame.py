"""
This is the script matching to DataFrameDataset.scala.
"""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("SparkDataFrame").master("local[*]").getOrCreate()
    people = spark.read.option("header", True).option("inferSchema", True).csv("./data/fakefriends.csv")
    print("Here is our inferred schema:")
    people.printSchema()

    print("Let's select the name column:")
    people.select("name").show()

    print("Filter out anyone over 21:")
    people.filter(people["age"] < 21).show()

    print("Group by age:")
    people.groupby("age").count().show()

    print("Make everyone 10 years older:")
    people.select(people["name"], people["age"] + 10).show()

    spark.stop()


if __name__ == "__main__":
    main()
