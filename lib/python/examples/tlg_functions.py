#!/usr/bin/env python

import os

from pathling.tlg import PathlingContext
from pyspark.sql.functions import *

HERE = os.path.abspath(os.path.dirname(__file__))


def main():
    pc = PathlingContext.create()

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

    # << 64572001 : (<< 370135005 = << 441862004 , << 363698007 = << 2095001 )
    # Infectiction in sinuses
    SINUS_INFECTION_VS = "http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2064572001%20%3A%20(" \
                         "%3C%3C%20370135005%20%3D%20%3C%3C%20441862004%20%2C%20%3C%3C" \
                         "%20363698007%20%3D%20%3C%3C%202095001%20)"

    pathling_ctx = PathlingContext(spark,
                                   serverUrl='https://tx.ontoserver.csiro.au/fhir')

    memberOf_df = pathling_ctx.memberOf(coding_df, col('coding'), SINUS_INFECTION_VS,
                                        'is_sinus_infection')
    result = memberOf_df.select('id',
                                col('coding').getField('code'),
                                col('coding').getField('display'),
                                col('is_sinus_infection')).toPandas()
    print(result)


if __name__ == "__main__":
    main()
