#!/usr/bin/env python

import os

from pyspark.sql import SparkSession

from pathling.etc import find_jar
from pathling.r4 import bundles

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    # Configure spark session to include pathling jar
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[*]') \
        .config('spark.jars', find_jar()) \
        .getOrCreate()

    json_resources_dir = os.path.join(HERE, 'data/resources/')

    # Load resource files in the ndjson format to the RDD of bundles
    resource_bundles_rdd = bundles.from_resource_json(
            spark.read.text(json_resources_dir),
            "value")

    # Extract 'Condition' resources from the RDD of bundles to a dataframe
    patients_df = bundles.extract_entry(spark, resource_bundles_rdd, 'Patient')
    patients_df.show()


if __name__ == "__main__":
    main()
