import re
import pyspark

from pyspark import SparkConf, SparkContext
from typing import Optional


BFSData = tuple[list[int], int, str]
BFSNode = tuple[int, BFSData]
START_CHARACTER_ID: int = 5306
TARGET_CHARACTER_ID: int = 14
HIT_COUNTER: Optional[pyspark.accumulators.Accumulator] = None


def convert_to_bfs(line: str) -> BFSNode:
    fields = list(filter(None, re.split(r"\s+", line)))
    hero_id = int(fields[0])
    connections = [int(x) for x in fields[1:]]
    color = "WHITE"
    distance = 9999
    if hero_id == START_CHARACTER_ID:
        color = "GRAY"
        distance = 0

    return hero_id, (connections, distance, color)


def create_starting_rdd(sc: SparkContext) -> pyspark.rdd.RDD:
    input_file = sc.textFile("./data/marvel-graph.txt")
    rdd = input_file.map(convert_to_bfs)

    return rdd


def bfs_map(node: BFSNode) -> list[BFSNode]:
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    results = []

    if color == "GRAY":
        for connection in connections:
            new_char_id = connection
            new_distance = distance + 1
            new_color = "GRAY"

            if connection == TARGET_CHARACTER_ID:
                if isinstance(HIT_COUNTER, pyspark.accumulators.Accumulator):
                    HIT_COUNTER.add(1)

            new_entry = (new_char_id, ([], new_distance, new_color))
            results.append(new_entry)

        color = "BLACK"

    this_entry = (character_id, (connections, distance, color))
    results.append(this_entry)

    return results


def bfs_reudce(data1: BFSData, data2: BFSData) -> BFSData:
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = "WHITE"
    edges = []

    if len(edges1) > 0:
        edges += edges1

    if len(edges2) > 0:
        edges += edges2

    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    if color1 == "WHITE" and (color2 in ["GRAY", "BLACK"]):
        color = color2

    if color1 == "GRAY" and color2 == "BLACK":
        color = color2

    if color2 == "WHITE" and (color1 in ["GRAY", "BLACK"]):
        color = color1

    if color2 == "GRAY" and color1 == "BLACK":
        color = color1

    if color1 == "GRAY" and color2 == "GRAY":
        color = color1

    if color1 == "BLACK" and color2 == "BLACk":
        color = color1

    return edges, distance, color


def main() -> None:
    global HIT_COUNTER, START_CHARACTER_ID, TARGET_CHARACTER_ID

    conf = SparkConf().setAppName("DegreesOfSeparation").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("Error")

    HIT_COUNTER = sc.accumulator(0)

    iteration_rdd = create_starting_rdd(sc)

    for iteration in range(10):
        print(f"Running BFS Iteration# {iteration + 1}")

        mapped = iteration_rdd.flatMap(bfs_map)
        print(f"Processing {mapped.count()} values.")

        if isinstance(HIT_COUNTER, pyspark.accumulators.Accumulator):
            hit_count = HIT_COUNTER.value
            if hit_count > 0:
                print(f"Hit the target character! From {hit_count} different direction(s).")
                return

        iteration_rdd = mapped.reduceByKey(bfs_reudce)


if __name__ == '__main__':
    main()
