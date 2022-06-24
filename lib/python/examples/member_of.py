#!/usr/bin/env python

import os

from pathling import PathlingContext
from pathling.functions import to_coding

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}')

# << 64572001|Disease| : (
#   << 370135005|Pathological process| = << 441862004|Infectious process|,
#   << 363698007|Finding site| = << 2095001|Nasal sinus structure|
# )
result = pc.member_of(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct?fhir_vs=ecl/%3C%3C%2064572001%20%3A%20('
                      '%3C%3C%20370135005%20%3D%20%3C%3C%20441862004%20%2C%20%3C%3C%20363698007'
                      '%20%3D%20%3C%3C%202095001%20)',
                      'result')
result.show()
