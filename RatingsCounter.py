from pyspark import SparkContext, SparkConf


def main() -> None:

    # Create the session
    conf = SparkConf().setAppName("RatingsCounter").setMaster("local[*]")

    # Create the context
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")

    # Read data
    lines = sc.textFile('./data/ml-100k/u.data')
    ratings = lines.map(lambda x: x.split('\t')[2])
    results = ratings.countByValue()
    sorted_results = sorted([item for item in results.items()], key=lambda x: x[0])
    for pair in sorted_results:
        print(pair)


if __name__ == "__main__":
    main()
