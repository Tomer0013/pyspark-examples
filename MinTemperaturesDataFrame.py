"""
This is the matching script to MinTemperaturesDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructType
from pyspark.sql.functions import *


def main() -> None:
    spark = SparkSession.builder.appName("MinTemperaturesDataset").master("local[*]").getOrCreate()

    temperature_schema = StructType()
    temperature_schema.add("station_id", StringType(), nullable=True)
    temperature_schema.add("date", IntegerType(), nullable=True)
    temperature_schema.add("measure_type", StringType(), nullable=True)
    temperature_schema.add("temperature", FloatType(), nullable=True)

    df = spark.read.format("csv").schema(temperature_schema).load("./data/1800.csv")
    min_temps = df.filter(col("measure_type") == "TMIN")
    station_temps = min_temps.select("station_id", "temperature")
    min_temp_by_station = station_temps.groupby("station_id").min("temperature")
    min_temp_by_station_f = min_temp_by_station.\
        withColumn("temperature", round(col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, scale=2)).\
        select("station_id", "temperature").sort("temperature")

    results = min_temp_by_station_f.collect()
    for result in results:
        print(f"{result[0]} minimum temperature: {result[1]:.2f} F")

    spark.stop()


if __name__ == "__main__":
    main()
