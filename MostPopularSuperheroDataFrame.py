"""
This script matches MostPopularSuperheroDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import split, col, sum, size


def main() -> None:
    spark = SparkSession.builder.appName("MostPopularSuperheroDataFrame").master("local[*]").getOrCreate()

    superhero_names_schema = StructType()
    superhero_names_schema.add("id", IntegerType(), True)
    superhero_names_schema.add("name", StringType(), True)
    names = spark.read.format("csv").schema(superhero_names_schema).option("sep", " ").load("./data/marvel-names.txt")

    lines = spark.read.text("./data/marvel-graph.txt")
    connections = lines.withColumn("id", split(col("value"), pattern=" ")[0]) \
                       .withColumn("connections", size(split(col("value"), pattern=" ")) - 1) \
                       .groupby("id").agg(sum("connections").alias("connections"))

    most_popular = connections.sort(col("connections").desc()).first()
    most_popular_name = names.filter(col("id") == most_popular[0]).select("name").first()
    print(f"{most_popular_name[0]} is the most popular superhero with {most_popular[1]} co-appearances.")

    spark.stop()


if __name__ == "__main__":
    main()
