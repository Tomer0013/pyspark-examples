"""
This script matches MostObscureSuperheroesDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import split, col, sum, size


def main() -> None:
    spark = SparkSession.builder.appName("MostObscureSuperheroesDataFrame").master("local[*]").getOrCreate()

    superhero_names_schema = StructType()
    superhero_names_schema.add("id", IntegerType(), True)
    superhero_names_schema.add("name", StringType(), True)
    names = spark.read.format("csv").schema(superhero_names_schema).option("sep", " ").load("./data/marvel-names.txt")

    lines = spark.read.text("./data/marvel-graph.txt")
    connections = lines.withColumn("id", split(col("value"), pattern=" ")[0]) \
                       .withColumn("connections", size(split(col("value"), pattern=" ")) - 1) \
                       .groupby("id").agg(sum("connections").alias("connections"))

    min_connections = connections.sort(col("connections")).first()[1]
    heroes_with_min_connections = connections.filter(col("connections") == min_connections) \
        .join(names, "id").select("name", "connections")

    heroes_with_min_connections.show(heroes_with_min_connections.count())

    spark.stop()


if __name__ == "__main__":
    main()
