#!/usr/bin/env python3.12
# -*- Coding: UTF-8 -*-
# @Time     :   2026/8/19 22:51
# @Author   :   Shawn
# @Version  :   Version 0.1.0
# @File     :   PS.py
# @Desc     :   

from faker import Faker
from pathlib import Path
from pprint import pprint
from pyspark.sql import SparkSession, DataFrame
from shutil import move, rmtree
from typing import Any, Literal, Self


class SparkEngineStarter:
    """ Context manager for starting and stopping a PySpark engine. """

    def __init__(self, app_name: str, *, master: str | Literal["local[*]"] = "local[*]", display: bool = True) -> None:
        """
        Initialise the Spark engine starter.
        :param app_name: Name of the Spark application.
        :param master: Spark master URL. Defaults to ``"local[*]"``.
        :param display: Whether to display engine-related information.
        """
        self._name = app_name
        self._master = master
        self._dis = display
        self._spark: SparkSession | None = None

    def _start(self) -> SparkSession:
        """
        Start and return a Spark session.
        :return: Spark session.
        """
        return SparkSession.builder.appName(self._name).master(self._master).getOrCreate()

    def start(self) -> SparkSession:
        """
        Start and return a Spark session.
        :return: Spark session.
        """
        self._spark = self._start()
        return self._spark

    @property
    def spark(self) -> SparkSession:
        """
        Return the Spark session.
        :return: Spark session.
        """
        if self._spark is None: raise RuntimeError("Spark engine has not been started.")
        return self._spark

    def __enter__(self) -> Self:
        """
        Start the Spark engine and enter the context.
        :return: Spark engine starter.
        """
        if self._dis: print(self)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Stop the Spark engine and exit the context.
        :param exc_type: Exception type.
        :param exc_value: Exception value.
        :param traceback: Traceback object.
        :return: Whether to suppress the exception.
        """
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def __repr__(self) -> str:
        """
        Return a string representation of the Spark engine starter.
        :return: String representation of the Spark engine starter.
        """
        return f"SparkEngineStarter(app_name={self._name!r}, master={self._master!r}, display={self._dis!r})"


def fake_data(amount: int = 10, *, language: str | Literal["zh_CN", "en_GB"] = "en_GB", display: bool = True) -> tuple:
    """
    Generate fake data.
    :param amount: The amount of data to generate.
    :param language: The language to generate data in.
    :param display: Whether to display the generated data.
    :return: The generated data and a Faker object.
    """
    fake: Faker = Faker(language)
    data: list[dict] = [
        {
            "name": fake.first_name(),
            "age": fake.random_int(min=18, max=24),
            "gender": fake.random_element(elements=("male", "female")),
            "score": fake.random_int(min=50, max=100),
        }
        for _ in range(amount)
    ]
    if display: pprint(data)
    return data, fake


def spark_csv(
        spark: SparkSession, filepath: str | Path,
        *,
        header: bool = True,
        infer_schema: bool = True,
        display: bool = True
) -> DataFrame:
    """
    Read a CSV dataset into a Spark DataFrame.
    :param spark: Active Spark session used to read the CSV data.
    :param filepath: Path to the CSV file or Spark CSV output directory.
    :param header: Whether the first row contains column names.
    :param infer_schema: Whether Spark should infer column data types.
    :param display: Whether to display information about the loaded data.
    :return: Spark DataFrame containing the CSV data.
    """
    dataframe: DataFrame = spark.read.csv(str(filepath), header=header, inferSchema=infer_schema)
    if display:
        print(f"CSV data loaded from: {filepath}")
        dataframe.printSchema()
        dataframe.show()
    return dataframe
