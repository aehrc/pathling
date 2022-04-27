#!/usr/bin/env python

import os

from pyspark.sql import SparkSession

from pathling.etc import find_jar
from pathling.tlg import PathlingContext

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    # Configure spark session to include pathling jar
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_jar()) \
        .getOrCreate()

    print(find_jar(True))

    pathling_ctx = PathlingContext(spark,
                                   serverUrl='https://tx.ontoserver.csiro.au/fhir')


if __name__ == "__main__":
    main()
