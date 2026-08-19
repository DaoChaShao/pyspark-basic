#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/19 23:20
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   02_data.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

from utils import fake_data, SparkEngineStarter


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"
    # engine: SparkEngineStarter = SparkEngineStarter(app_name=app_name)
    # spark: SparkSession = engine.start()
    spark: SparkSession = SparkEngineStarter(app_name=app_name).start()

    # Create a data
    data, _ = fake_data(10, display=False)

    # Create a DataFrame
    schema: list[str] = ["name", "age", "gender", "score"]
    df: DataFrame = spark.createDataFrame(data, samplingRatio=0.5).select(schema)
    df.show(5)

    schema: StructType = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("score", IntegerType(), True)
    ])
    df: DataFrame = spark.createDataFrame(data, schema=schema, verifySchema=True)
    print(df.toPandas())


if __name__ == "__main__":
    main()
