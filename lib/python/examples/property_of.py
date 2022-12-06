#!/usr/bin/env python

#  Copyright 2022 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os

from pathling import PathlingContext
from pathling.functions import to_snomed_coding
from pathling.udfs import property_of, PropertyType

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
        f'file://{os.path.join(HERE, "data/csv/conditions.csv")}'
)

# Obtain parents of the codes

result = csv.withColumn("PARENT",
                        property_of(to_snomed_coding(csv.CODE), "parent", PropertyType.CODE))
result.select("CODE", "DESCRIPTION", "PARENT").show()
