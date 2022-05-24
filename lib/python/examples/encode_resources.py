#!/usr/bin/env python

import os

from pyspark.sql import SparkSession

from pathling import PathlingContext
from pathling.etc import find_jar

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    # Configure spark session to include pathling jar
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[*]') \
        .config('spark.jars', find_jar()) \
        .getOrCreate()

    json_resources_dir = os.path.join(HERE, 'data/resources/')
    spark.read.text(json_resources_dir).show()

    plc = PathlingContext.create(spark)
    patients_df = plc.encode(spark.read.text(json_resources_dir), 'Patient')
    patients_df.show()


if __name__ == "__main__":
    main()
