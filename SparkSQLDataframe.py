"""
This is the script matching to SparkSQLDataset.scala. We don't have Datasets in PySpark. Only Dataframes.
"""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("SparkSQLDataframe").master("local[*]").getOrCreate()
    schema_people = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/fakefriends.csv")
    schema_people.printSchema()
    schema_people.createOrReplaceTempView("people")
    teenagers = spark.sql(
        """
        SELECT
            *
        FROM
            people
        WHERE
            age >= 13 AND age <= 19
        """
    )
    results = teenagers.collect()
    for result in results:
        print(result)

    spark.stop()


if __name__ == "__main__":
    main()
