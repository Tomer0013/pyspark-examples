"""
This script matches LinearRegressionDataFrameDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def main() -> None:
    spark = SparkSession.builder.appName("LinearRegressionExample").master("local[*]").getOrCreate()
    regression_schema = StructType()\
        .add("label", DoubleType(), True)\
        .add("features_raw", DoubleType(), True)

    ds_raw = spark.read.option("sep", ",").schema(regression_schema).csv("./data/regression.txt")
    assembler = VectorAssembler().setInputCols(["features_raw"]).setOutputCol("features")
    df = assembler.transform(ds_raw).select("label", "features")

    train_df, test_df = df.randomSplit([0.5, 0.5])

    lir = LinearRegression().setRegParam(0.3).setElasticNetParam(0.8).setMaxIter(100).setTol(1e-6)
    model = lir.fit(train_df)

    # This basically adds a "prediction" column to our testDF dataframe.
    # Cache because in real situations you usually want to use the preds again and again
    full_preds = model.transform(test_df).cache()
    pred_and_label = full_preds.select("prediction", "label").collect()
    for tup in pred_and_label:
        print(f"{tup[0]}, {tup[1]}")

    spark.stop()


if __name__ == '__main__':
    main()
