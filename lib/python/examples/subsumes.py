#!/usr/bin/env python

import os

from pathling import PathlingContext
from pathling.coding import Coding
from pathling.functions import to_coding

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}').limit(3)
csv = csv.selectExpr('CODE as LEFT').crossJoin(csv.selectExpr('CODE as RIGHT'))

result_1 = pc.subsumes(csv, 'SUBSUMES',
                       left_coding_column=to_coding(csv.LEFT, 'http://snomed.info/sct'),
                       right_coding_column=to_coding(csv.RIGHT, 'http://snomed.info/sct'))
result_2 = pc.subsumes(result_1, 'LEFT_IS_FRACTURE',
                       left_coding=Coding('http://snomed.info/sct', '125605004'),
                       right_coding_column=to_coding(csv.LEFT, 'http://snomed.info/sct'))
result_2.show()
