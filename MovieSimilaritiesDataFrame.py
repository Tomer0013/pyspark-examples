"""
This script matches MovieSimilaritiesDataset.scala.
"""
import argparse
import pyspark
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, LongType


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-movie_id",
                        type=int,
                        required=True,
                        help="Movie ID of the movie to compute the similar movies for.")
    args = parser.parse_args()

    return args


def compute_cosine_similarity(data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    pair_scores = data.\
        withColumn("xx", sf.col("rating1") * sf.col("rating1")).\
        withColumn("yy", sf.col("rating2") * sf.col("rating2")).\
        withColumn("xy", sf.col("rating1") * sf.col("rating2"))

    calculate_similarity = pair_scores.groupby("movie1", "movie2").\
        agg(
        sf.sum(sf.col("xy")).alias("numerator"),
        (sf.sqrt(sf.sum(sf.col("xx"))) * sf.sqrt(sf.sum(sf.col("yy")))).alias("denominator"),
        sf.count(sf.col("xy")).alias("numPairs")
    )

    result = calculate_similarity.\
        withColumn("score",
                   sf.when(sf.col("denominator") != 0, sf.col("numerator") / sf.col("denominator")).otherwise(None)
                   ).select("movie1", "movie2", "score", "numPairs")

    return result


def get_movie_name(movie_names: pyspark.sql.DataFrame, movie_id: int) -> str:
    result = str(movie_names.filter(sf.col("movieID") == movie_id).select("movieTitle").collect()[0].movieTitle)

    return result


def main(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("MovieSimilaritiesDataFrame").master("local[*]").getOrCreate()

    movie_names_schema = StructType()
    movie_names_schema.add("movieID", IntegerType(), True)
    movie_names_schema.add("movieTitle", StringType(), True)

    movie_schema = StructType()
    movie_schema.add("userID", IntegerType(), True)
    movie_schema.add("movieID", IntegerType(), True)
    movie_schema.add("rating", IntegerType(), True)
    movie_schema.add("timestamp", LongType(), True)

    print("Loading movie names...")
    movie_names = spark.read.option("sep", "|").option("charset", "ISO-8859-1").schema(movie_names_schema).\
        csv("./data/ml-100k/u.item")
    movies = spark.read.option("sep", "\t").schema(movie_schema).csv("./data/ml-100k/u.data")
    ratings = movies.select("userID", "movieID", "rating")

    movie_pairs = ratings.alias("ratings1").\
        join(ratings.alias("ratings2"), (sf.col("ratings1.userID") == sf.col("ratings2.userID")) &
             (sf.col("ratings1.movieID") < sf.col("ratings2.movieID"))).\
        select(sf.col("ratings1.movieID").alias("movie1"),
               sf.col("ratings2.movieID").alias("movie2"),
               sf.col("ratings1.rating").alias("rating1"),
               sf.col("ratings2.rating").alias("rating2"))

    movie_pair_similarities = compute_cosine_similarity(movie_pairs).cache()

    score_threshold = 0.97
    co_occurrence_threshold = 50.0
    movie_id = args.movie_id

    filtered_results = movie_pair_similarities.filter(
        ((sf.col("movie1") == movie_id) | (sf.col("movie2") == movie_id)) &
        (sf.col("score") > score_threshold) & (sf.col("numPairs") > co_occurrence_threshold))

    results = filtered_results.sort(sf.col("score").desc()).take(10)
    print(f"\nTop 10 similar movies for {get_movie_name(movie_names, movie_id)}")
    for result in results:
        similar_movie_id = result.movie1
        if similar_movie_id == movie_id:
            similar_movie_id = result.movie2
        print(f"{get_movie_name(movie_names, similar_movie_id)}\tscore: {result.score}\tstrength: {result.numPairs}")


if __name__ == '__main__':
    main(get_args())
