#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 01:53
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   09_read_csv.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame

from utils import SparkEngineStarter, spark_csv


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"

    with SparkEngineStarter(app_name=app_name) as engine:
        spark: SparkSession = engine.start()
        # Read dataframe into CSV file
        # df: DataFrame = spark_csv(spark, "data/pandas_students.csv")
        df: DataFrame = spark_csv(spark, "data/spark_students.csv")
        df.show()
        df.printSchema()


if __name__ == "__main__":
    main()
