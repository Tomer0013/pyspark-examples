# We want to count and sort on the cluster, because it is not guaranteed that the driver's computer can handle
# all the data. That is why we are not using countByValue as in previous example. countByValue collects
# all the data to the driver computer and returns a dict. We will also not collect at the end of the script.

import re

from pyspark import SparkConf, SparkContext


def main() -> None:
    conf = SparkConf().setAppName("WordCountBetterSorted").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")
    input = sc.textFile("./data/book.txt")
    words = input.flatMap(lambda x: list(filter(None, re.split(r'\W+', x))))
    lower_case_words = words.map(lambda x: x.lower())
    word_counts = lower_case_words.map(lambda x: (x, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    word_counts_sorted = word_counts.map(lambda tup: (tup[1], tup[0])).sortByKey()
    word_counts_sorted.foreach(lambda tup: print(f"{tup[1]}: {tup[0]}"))


if __name__ == "__main__":
    main()
