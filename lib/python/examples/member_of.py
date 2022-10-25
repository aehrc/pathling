#!/usr/bin/env python

import os

from pathling import PathlingContext
from pathling.functions import to_coding, to_ecl_value_set

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}')

result = pc.member_of(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      to_ecl_value_set("""
<< 64572001|Disease| : (
  << 370135005|Pathological process| = << 441862004|Infectious process|,
  << 246075003|Causative agent| = << 49872002|Virus|
)
                      """), 'VIRAL_INFECTION')
result.select('CODE', 'DESCRIPTION', 'VIRAL_INFECTION').show()
