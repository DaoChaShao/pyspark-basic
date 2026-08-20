#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 19:01
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   15_join.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

from utils import SparkEngineStarter


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"

    # Create a data
    info_data: list[tuple[str, int, str]] = [
        ("Alice", 20, "Male"),
        ("Bob", 21, "Male"),
        ("Charlie", 19, "Female"),
    ]
    score_data: list[tuple[str, int]] = [
        ("Alice", 85),
        ("Bob", 92),
        ("Charlie", 78),
    ]

    info_schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True)
    ])
    score_schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("score", IntegerType(), True)
    ])

    with SparkEngineStarter(app_name=app_name) as engine:
        spark: SparkSession = engine.start()
        info: DataFrame = spark.createDataFrame(info_data, schema=info_schema, verifySchema=True)
        score: DataFrame = spark.createDataFrame(score_data, schema=score_schema, verifySchema=True)
        info.show()
        score.show()

        df = info.join(score, on="name", how="inner")  # "inner", "left", "right", "outer"
        df.show()


if __name__ == "__main__":
    main()
