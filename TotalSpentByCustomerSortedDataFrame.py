"""
This script matches TotalSpentByCustomerDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, FloatType
import pyspark.sql.functions as sf


def main() -> None:
    spark = SparkSession.builder.appName("TotalSpentByCustomerDataFrame").master("local[*]").getOrCreate()

    order_schema = StructType()
    order_schema.add("customerID", IntegerType(), True)
    order_schema.add("itemID", IntegerType(), True)
    order_schema.add("amount", FloatType(), True)

    df = spark.read.format("csv").schema(order_schema).load("./data/customer-orders.csv")
    customer_spent = df.groupby("customerID")\
        .agg(sf.round(sf.sum("amount")).alias("total_spent"))\
        .sort("total_spent", ascending=False)

    results = customer_spent.collect()
    for res in results:
        print(f"Customer {res[0]}: {res[1]}")

    spark.stop()


if __name__ == "__main__":
    main()
