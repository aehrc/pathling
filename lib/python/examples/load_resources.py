#!/usr/bin/env python

import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession

from pathling.etc import find_jar
from pathling.r4 import bundles

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))


def main():
    warehouse_dir = mkdtemp()
    print(warehouse_dir)

    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_jar()) \
        .config('spark.sql.warehouse.dir', warehouse_dir) \
        .getOrCreate()

    json_resources_dir = os.path.join(PROJECT_DIR,
                                      'encoders/src/test/resources/data/resources/R4/json/')

    resource_bundles = bundles.from_resource_json(
        spark.read.text(json_resources_dir),
        "value")
    conditions = bundles.extract_entry(spark, resource_bundles, 'Condition')
    conditions.show()

    patients = bundles.extract_entry(spark, resource_bundles, 'Patient')
    patients.show()


if __name__ == "__main__":
    main()
