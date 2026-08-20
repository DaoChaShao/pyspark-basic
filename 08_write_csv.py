#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/20 01:46
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   08_write_csv.py
# @Desc     :   

from pandas import DataFrame as PDF
from pyspark.sql import SparkSession, DataFrame as SDF
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
        df: SDF = spark.createDataFrame(data, schema=schema, verifySchema=True)

        # Save dataframe into CSV file
        df.write.csv("data/spark_students.csv", mode="overwrite", header=True)

    # Build a Pandas dataframe and save a CSV file
    pdf: PDF = PDF(data)
    pdf.to_csv("data/pandas_students.csv", index=False)
    print(pdf)


if __name__ == "__main__":
    main()
