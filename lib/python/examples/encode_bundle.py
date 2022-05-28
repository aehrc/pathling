#!/usr/bin/env python

import os

from pyspark.sql import SparkSession

from pathling import PathlingContext
from pathling.etc import find_jar
from pathling.fhir import Version, MimeType

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    # Configure spark session to include pathling jar
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[*]') \
        .config('spark.jars', find_jar()) \
        .getOrCreate()

    json_bundles_dir = os.path.join(HERE, 'data/bundles/')

    plc = PathlingContext.create(spark, fhirVersion=Version.R4)

    json_bundles_df = spark.read.text(json_bundles_dir, wholetext=True)
    json_bundles_df.show()

    # Extract 'Patient' resources from the RDD of bundls to a dataframe
    # with FHIR extension support on.
    condititions_df = plc.encodeBundle(json_bundles_df, 'Patient', inputType=MimeType.FHIR_JSON)
    condititions_df.show()


if __name__ == "__main__":
    main()
