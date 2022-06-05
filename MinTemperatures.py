from pyspark import SparkConf, SparkContext


def parse_line(line: str) -> tuple:
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0

    return station_id, entry_type, temperature


def main() -> None:
    conf = SparkConf().setAppName("MinTemperatures").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")
    lines = sc.textFile("data/1800.csv")
    parsed_lines = lines.map(parse_line)
    min_temps = parsed_lines.filter(lambda x: x[1] == "TMIN")
    station_temps = min_temps.map(lambda x: (x[0], float(x[2])))
    min_temps_by_station = station_temps.reduceByKey(lambda x, y: min(x, y))
    results = min_temps_by_station.collect()
    for result in sorted(results, key=lambda x: x[0]):
        station = result[0]
        temp = result[1]
        print(f"{station} minimum temperature: {temp:.2f} F")


if __name__ == "__main__":
    main()
