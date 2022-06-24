#!/usr/bin/env python

import os

from pathling import PathlingContext
from pathling.functions import to_coding

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}').limit(3)
csv = csv.selectExpr('CODE as LEFT').crossJoin(csv.selectExpr('CODE as RIGHT'))

result = pc.subsumes(csv, to_coding(csv.LEFT, 'http://snomed.info/sct'),
                     to_coding(csv.RIGHT, 'http://snomed.info/sct'), 'result')
result.show()
