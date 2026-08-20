#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 19:21
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   16_window.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, avg, round
from pyspark.sql.window import Window, WindowSpec
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

        window: WindowSpec = Window.partitionBy("gender").orderBy(col("score").desc())
        df: DataFrame = df.withColumn(
            "rank",
            row_number().over(window)
        )
        df.show()

        window: WindowSpec = Window.partitionBy("gender")
        df: DataFrame = df.withColumn(
            "gender_avg",
            round(avg("score").over(window), 2)
        )
        df.show()


if __name__ == "__main__":
    main()
