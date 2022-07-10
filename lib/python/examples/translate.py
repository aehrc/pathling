#!/usr/bin/env python

import os

from pathling import PathlingContext
from pathling.functions import to_coding

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}')

# Translate codings to Read CTV3 using the map that ships with SNOMED CT.
result = pc.translate(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000',
                      output_column_name='READ_CODE')
result = result.withColumn('READ_CODE', result.READ_CODE.code)
result.select('CODE', 'DESCRIPTION', 'READ_CODE').show()
