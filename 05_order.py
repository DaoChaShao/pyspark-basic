#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 01:29
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   05_order.py
# @Desc     :   

from pyspark.sql import SparkSession, DataFrame
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
        df: DataFrame = df.withColumn("eval", df.score > 85)
        df.orderBy("score").show()


if __name__ == "__main__":
    main()
