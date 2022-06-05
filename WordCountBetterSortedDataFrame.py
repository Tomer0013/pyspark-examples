"""
This script matches WordCountBetterSortedDataset.scala.
"""
import re
import pyspark.sql.functions as funcs

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def main() -> None:
    spark = SparkSession.builder.appName("WordCountBetterSortedDataFrame").master("local[*]").getOrCreate()
    input = spark.read.text("data/book.txt")
    words = input.select(funcs.explode(funcs.split("value", "\\W+")).alias("word")).filter(funcs.col("word") != "")
    lowercase_words = words.select(funcs.lower(funcs.col("word")).alias("word"))
    word_counts = lowercase_words.groupby("word").count()
    word_counts_sorted = word_counts.sort("count")
    word_counts_sorted.show(word_counts_sorted.count())

    # Another way, using DFs and RRDs:
    book_rdd = spark.sparkContext.textFile("./data/book.txt")
    words_rdd = book_rdd.flatMap(lambda x: list(filter(None, re.split(r'\W+', x))))
    words_df = words_rdd.toDF(StringType())
    word_count_sorted_df = words_df.select(funcs.lower(funcs.col("value")).alias("word")).\
        groupby("word").count().sort("count")
    word_count_sorted_df.show(word_count_sorted_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
