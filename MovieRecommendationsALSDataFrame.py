"""
This script matches MovieRecommendationsALSDataset.scala.
"""
import argparse
import pyspark.sql.functions as sf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, LongType


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-user_id",
                        type=int,
                        required=True,
                        help="User ID of the user to create recommendations for.")
    args = parser.parse_args()

    return args


def main(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("MovieRecommendationsALSDataFrame").master("local[*]").getOrCreate()

    movie_names_schema = StructType()
    movie_names_schema.add("movieID", IntegerType(), True)
    movie_names_schema.add("movieTitle", StringType(), True)

    movie_schema = StructType()
    movie_schema.add("userID", IntegerType(), True)
    movie_schema.add("movieID", IntegerType(), True)
    movie_schema.add("rating", IntegerType(), True)
    movie_schema.add("timestamp", LongType(), True)

    print("Loading movie names...")
    names = spark.read.option("sep", "|").option("charset", "ISO-8859-1")\
        .schema(movie_names_schema).csv("./data/ml-100k/u.item")
    names_list = names.collect()
    movie_id_to_name_dict = {row.movieID: row.movieTitle for row in names_list}

    print("Loading ratings data...")
    ratings = spark.read.option("sep", "\t").schema(movie_schema).csv("./data/ml-100k/u.data")

    print("Training recommendation model...")
    als = ALS().setMaxIter(10).setRegParam(0.05).setAlpha(40).setRank(100)\
        .setUserCol("userID").setItemCol("movieID").setRatingCol("rating")
    model = als.fit(ratings)

    print(f"Creating top 10 recommendations for user {args.user_id}")
    user_id_df = ratings.select(als.getUserCol()).distinct().filter(sf.col(als.getUserCol()) == args.user_id)
    recommendations = model.recommendForUserSubset(user_id_df, 10).collect()
    for recommendation in recommendations:
        for rec_row in recommendation.recommendations:
            print(f"{movie_id_to_name_dict[rec_row.movieID]}, {rec_row.rating}")

    spark.stop()


if __name__ == '__main__':
    main(get_args())
