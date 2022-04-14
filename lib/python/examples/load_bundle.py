#!/usr/bin/env python

import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession

from pathling.r4 import bundles
from pathling.etc import find_jar

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))


def main():
    print(PROJECT_DIR)

    warehouse_dir = mkdtemp()
    print(warehouse_dir)

    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_jar()) \
        .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
        .config('spark.sql.warehouse.dir', warehouse_dir) \
        .getOrCreate()

    json_bundles_dir = os.path.join(PROJECT_DIR,
                                    'encoders/src/test/resources/data/bundles/R4/json/')

    json_bundles = bundles.load_from_directory(spark, json_bundles_dir)

    conditions = bundles.extract_entry(spark, json_bundles, 'Condition')
    conditions.show()

    patients = bundles.extract_entry(spark, json_bundles, 'Patient')
    patients.show()


if __name__ == "__main__":
    main()
