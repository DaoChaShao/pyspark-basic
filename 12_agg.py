#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 01:33
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   12_agg.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, max, min, sum, count, mean
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

from utils import fake_data, SparkEngineStarter


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"

    # Create a data
    data, _ = fake_data(10, display=False)

    schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("score", IntegerType(), True)
    ])
    with SparkEngineStarter(app_name=app_name) as engine:
        spark: SparkSession = engine.start()
        df: DataFrame = spark.createDataFrame(data, schema=schema, verifySchema=True)

        # Polymerisation
        df.agg(avg(col("score")).alias("avg_score")).show()
        df.groupBy("gender").agg(avg(col("score")).alias("avg_score")).show()
        df.agg(
            avg("score"),
            max("score"),
            min("score"),
            sum("score"),
            count("score")
        ).show()

        age_mean = df.select(mean("age")).first()
        print(age_mean)
        age_mean_value = df.select(mean("age")).first()[0]
        print(age_mean_value)


if __name__ == "__main__":
    main()
