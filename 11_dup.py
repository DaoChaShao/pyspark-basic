#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 17:53
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   11_dup.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

from utils import fake_data, SparkEngineStarter, spark_csv


def main() -> None:
    """ Main Function """
    # Create a Spark Session
    app_name: str = "students"

    with SparkEngineStarter(app_name=app_name) as engine:
        spark: SparkSession = engine.start()
        # Read dataframe into CSV file
        df: DataFrame = spark_csv(spark, "data/spark_students.csv")
        # Drop entire same
        print(f"Before Distinct: {df.count()}")
        df: DataFrame = df.distinct()
        print(f"After Distinct: {df.count()}")

        # Drop duplicate rows
        print(f"Before Drop Duplicates: {df.count()}")
        df: DataFrame = df.drop_duplicates(["age"])
        print(f"After Drop Duplicates: {df.count()}")


if __name__ == "__main__":
    main()
