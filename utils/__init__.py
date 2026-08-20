#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/19 22:49
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   __init__.py.py
# @Desc     :   


"""

****************************************************************
PySpark Utility Toolkit
----------------------------------------------------------------
This module provides utility functions for configuring and
initialising PySpark applications, including Spark engine
startup and related helper functionality.
****************************************************************
"""

__author__ = "Shawn Yu"
__version__ = "0.1.0"

from .PS import (SparkEngineStarter,
                 fake_data,
                 spark_csv, spark_json, spark_parquet)

__all__ = [
    "SparkEngineStarter",
    "fake_data",
    "spark_csv", "spark_json", "spark_parquet"
]
