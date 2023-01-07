#  Copyright 2023 Commonwealth Scientific and Industrial Research
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

from pyspark.sql.functions import explode_outer

from pathling import PathlingContext, to_snomed_coding, translate

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
    f'file://{os.path.join(HERE, "data/csv/conditions.csv")}'
)

# Translate codings to Read CTV3 using the map that ships with SNOMED CT.

result = csv.withColumn(
    "READ_CODES",
    translate(
        to_snomed_coding(csv.CODE),
        "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000",
    ).code,
)
result.select(
    "CODE", "DESCRIPTION", explode_outer("READ_CODES").alias("READ_CODE")
).show()
