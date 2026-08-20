#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 17:55
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   13_na.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import mode, mean
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

from utils import fake_data, SparkEngineStarter, spark_csv


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"

    # Create a data
    data = [
        ("Alice", None, "Male", 85),
        ("Bob", 21, None, 92),
        ("Charlie", 19, "Female", None),
        ("David", 20, "Male", 78),
        ("Eve", 22, "Female", 88),
        ("Frank", 21, "Male", 95),
        ("Grace", 20, "Female", 82),
    ]

    schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("score", IntegerType(), True)
    ])

    with SparkEngineStarter(app_name=app_name) as engine:
        spark: SparkSession = engine.start()
        df: DataFrame = spark.createDataFrame(data, schema=schema, verifySchema=True)
        # df.show()

        # Drop na row in age
        df: DataFrame = df.dropna(subset=["age"])
        # df.show()

        # Fill na
        gender_mode: str = df.select(mode("gender")).first()[0]
        score_mean: float = df.select(mean("score")).first()[0]
        df: DataFrame = df.fillna({"score": score_mean, "gender": gender_mode})
        df.show()


if __name__ == "__main__":
    main()
