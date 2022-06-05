"""
This script matches DecisionTreeDataset.scala.
"""

from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def main() -> None:
    spark = SparkSession.builder.appName("DecisionTreeExample").master("local[*]").getOrCreate()

    # Get data
    df_raw = spark.read.option("sep", ",").option("header", True).option("inferSchema", True).\
        csv("./data/realestate.csv")

    # Build pipeline
    feat_cols = [col for col in df_raw.columns if col not in ["No", "PriceOfUnitArea"]]
    assembler = VectorAssembler().setInputCols(feat_cols).setOutputCol("features")
    dt = DecisionTreeRegressor().setLabelCol("PriceOfUnitArea").setFeaturesCol("features")
    pipeline = Pipeline().setStages([assembler, dt])

    # Split to train test, fit and predict
    train_df, test_df = df_raw.randomSplit([0.7, 0.3])
    model = pipeline.fit(train_df)
    preds = model.transform(test_df)

    # Evaluate with RMSE
    evaluator = RegressionEvaluator().setLabelCol("PriceOfUnitArea").setPredictionCol("prediction").setMetricName("rmse")
    rmse = evaluator.evaluate(preds)
    print(f"RMSE on test data: {rmse}")

    spark.stop()


if __name__ == '__main__':
    main()
