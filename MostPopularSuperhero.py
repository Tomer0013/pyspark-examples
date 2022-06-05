from pyspark import SparkConf, SparkContext
from typing import Union
import re


def parse_names(line: str) -> Union[tuple[int, str], None]:
    fields = line.split('\"')
    if len(fields) > 1:
        return int(fields[0].strip()), fields[1]


def count_co_occurrences(line: str) -> tuple[int, int]:
    elements = list(filter(None, re.split(r'\W+', line)))

    return int(elements[0]), len(elements) - 1


def main() -> None:
    conf = SparkConf().setAppName("MostPopularSuperhero").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")

    names = sc.textFile("./data/marvel-names.txt")
    names_rdd = names.map(parse_names)

    lines = sc.textFile("./data/marvel-graph.txt")
    pairings = lines.map(count_co_occurrences)

    total_friends_by_character = pairings.reduceByKey(lambda v1, v2: v1 + v2)
    most_popular = total_friends_by_character.map(lambda x: (x[1], x[0])).max()
    most_popular_name = names_rdd.lookup(most_popular[1])[0]
    print(f"{most_popular_name} is the most popular superhero with {most_popular[0]} co-appearances.")


if __name__ == "__main__":
    main()
