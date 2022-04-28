#!/usr/bin/env python

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pathling.etc import find_jar
from pathling.r4 import bundles
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

    json_resources_dir = "../../../encoders/src/test/resources/data/resources/R4/json/"

    # Load resource files in the ndjson format to the RDD of bundles
    resource_bundles_rdd = bundles.from_resource_json(
        spark.read.text(json_resources_dir),
        "value")

    # Extract 'Condition' resources from the RDD of bundles to a dataframe
    conditions_df = bundles.extract_entry(spark, resource_bundles_rdd, 'Condition')
    coding_df = conditions_df.select(col("id"),
                                     explode(col("code").getField("coding")).alias("coding"))
    print(coding_df.toPandas())

    pathling_ctx = PathlingContext(spark,
                                   serverUrl='https://tx.ontoserver.csiro.au/fhir')

    TO_READ_CONCEPT_MAP_URI = "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000";

    translated_df = pathling_ctx.translate(coding_df, col('coding'), TO_READ_CONCEPT_MAP_URI,
                                           outputColumnName='readCoding')
    result = translated_df.select('id',
                                  col('coding').getField('code'),
                                  col('coding').getField('display'),
                                  col('readCoding').getField('code'),
                                  ).toPandas()
    print(result)


if __name__ == "__main__":
    main()
