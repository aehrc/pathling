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

    json_bundles_dir = os.path.join(HERE, 'data/bundles/')

    # Load R4 json bundles from a directory the RDD of bundles
    json_bundles_rdd = bundles.load_from_directory(spark, json_bundles_dir)

    # Extract 'Condition' resources from the RDD of bundls to a dataframe
    conditions_df = bundles.extract_entry(spark, json_bundles_rdd, 'Condition')
    conditions_df.show()

    # Extract 'Patient' resources from the RDD of bundls to a dataframe
    # with FHIR extension support on.
    patients_df = bundles.extract_entry(spark, json_bundles_rdd, 'Patient', enableExtensions=True)
    patients_df.show()


if __name__ == "__main__":
    main()
