#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/19 22:47
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   01_engine.py
# @Desc     :   

from pyspark.sql import SparkSession


def main() -> None:
    """ Main Function """
    spark: SparkSession = (
        SparkSession.builder
        .appName("Example")
        .master("local[*]")
        .getOrCreate()
    )
    print(spark)


if __name__ == "__main__":
    main()
