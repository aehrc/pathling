#!/usr/bin/env python

import os

from pyspark.sql.functions import col, explode

from pathling import PathlingContext

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    pc = PathlingContext.create(serverUrl='https://tx.ontoserver.csiro.au/fhir')

    # Read each line from the NDJSON into a row within a Spark data set.
    ndjson_dir = os.path.join(HERE, 'data/resources/')
    json_resources = pc.spark.read.text(ndjson_dir)

    # Extract 'Condition' resources from the RDD of bundles to a dataframe
    conditions_df = pc.encode(json_resources, 'Condition')
    coding_df = conditions_df.select(col("id"),
                                     explode(col("code").getField("coding")).alias("coding"))

    print(coding_df.toPandas())

    pathling_ctx = PathlingContext(spark,
                                   serverUrl='https://tx.ontoserver.csiro.au/fhir')

    TO_READ_CONCEPT_MAP_URI = "http://snomed.info/sct/900000000000207008?fhir_cm" \
                              "=900000000000497000";

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
