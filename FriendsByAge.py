from pyspark import SparkConf, SparkContext


def parse_line(line: str) -> tuple:
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])

    return age, num_friends


def main() -> None:
    conf = SparkConf().setAppName("FriendsByAge").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")

    lines = sc.textFile("./data/fakefriends-noheader.csv")
    rdd = lines.map(parse_line)
    totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_by_age = totals_by_age.mapValues(lambda x: int(x[0] / x[1]))
    results = sorted(average_by_age.collect(), key=lambda x: x[0])
    for pair in results:
        print(pair)


if __name__ == "__main__":
    main()
