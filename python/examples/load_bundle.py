#!/usr/bin/env python

from pyspark.sql import SparkSession
from tempfile import mkdtemp
from pathling.r4 import bundles


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


    json_bundles = bundles.load_from_directory(spark, '../../api/src/test/resources/data/bundles/R4/json/')

    conditions = bundles.extract_entry(spark, json_bundles, 'Condition')
    conditions.show()

    patients = bundles.extract_entry(spark, json_bundles, 'Patient')
    patients.show()

if __name__ == "__main__":
    main()
