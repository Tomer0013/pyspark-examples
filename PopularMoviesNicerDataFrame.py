"""
This script matches PopularMoviesNicerDataset.scala
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, LongType, StructType, StringType


def load_movie_names() -> dict:
    movie_id_to_title_dict = {}
    with open("./data/ml-100k/u.item", "r", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split("|")
            if len(fields) > 1:
                movie_id_to_title_dict[int(fields[0])] = fields[1]

    return movie_id_to_title_dict


def main() -> None:
    spark = SparkSession.builder.appName("PopularMoviesNicerDataFrame").master("local[*]").getOrCreate()
    name_dict = spark.sparkContext.broadcast(load_movie_names())

    movie_schema = StructType()
    movie_schema.add("user_id", IntegerType(), True)
    movie_schema.add("movie_id", IntegerType(), True)
    movie_schema.add("rating", IntegerType(), True)
    movie_schema.add("timestamp", LongType(), True)
    movies = spark.read.format("csv").option("sep", "\t").schema(movie_schema).load("./data/ml-100k/u.data")

    movie_counts = movies.groupby("movie_id").count()
    lookup_name = udf(lambda x: name_dict.value[x], StringType())
    movies_with_names = movie_counts.withColumn("movie_title", lookup_name(col("movie_id"))).sort("count")

    movies_with_names.show(movies_with_names.count(), truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
