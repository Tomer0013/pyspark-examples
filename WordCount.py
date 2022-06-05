from pyspark import SparkConf, SparkContext


def main() -> None:
    conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")
    input = sc.textFile("./data/book.txt")
    words = input.flatMap(lambda x: x.split(" "))
    word_counts = words.countByValue()
    for word_count in word_counts.items():
        print(word_count)


if __name__ == "__main__":
    main()
