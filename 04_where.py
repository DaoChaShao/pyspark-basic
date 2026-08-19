#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 01:05
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   04_where.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
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

        # Create new column and relevant dataframe
        df: DataFrame = df.withColumn("eval", col("score") > 85)
        df.where(df.eval == True).show()
        df.where(df.eval).show()
        df.where(col("eval")).show()


if __name__ == "__main__":
    main()
