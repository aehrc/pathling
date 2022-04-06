#!/usr/bin/env python

from tempfile import mkdtemp

from pathling.r4 import bundles
from pyspark.sql import SparkSession


def main():
    shaded_jar = '../../api/target/pathling-api-5.0.0-all.jar'
    warehouse_dir = mkdtemp()
    print(warehouse_dir)

    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', shaded_jar) \
        .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
        .config('spark.sql.warehouse.dir', warehouse_dir) \
        .getOrCreate()

    resource_bundles = bundles.from_resource_json(
        spark.read.text("../../api/src/test/resources/data/resources/R4/json/"),
        "value")
    conditions = bundles.extract_entry(spark, resource_bundles, 'Condition')
    conditions.show()


    patients = bundles.extract_entry(spark, resource_bundles, 'Patient')
    patients.show()


if __name__ == "__main__":
    main()
