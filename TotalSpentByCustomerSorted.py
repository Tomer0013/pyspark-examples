from pyspark import SparkConf, SparkContext


def parse_line(line: str) -> tuple:
    fields = line.split(",")
    customer_id, amount = int(fields[0]), float(fields[2])

    return customer_id, amount


def main() -> None:
    conf = SparkConf().setAppName("TotalSpentByCustomerSorted").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("./data/customer-orders.csv")
    customer_spent = lines.map(parse_line).reduceByKey(lambda v1, v2: v1 + v2)
    customer_spent_sorted = customer_spent.map(lambda x: (x[1], x[0])).sortByKey(ascending=False, numPartitions=1)
    results = customer_spent_sorted.collect()
    for result in results:
        print(f"Customer {result[1]}: {result[0]:.2f}")


if __name__ == "__main__":
    main()
