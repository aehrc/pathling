#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

from pathling import PathlingContext, Coding, to_coding, subsumes

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
    f'file://{os.path.join(HERE, "data/csv/conditions.csv")}'
)
first_3 = csv.limit(3)
cross_join = first_3.selectExpr(
    "CODE as LEFT", "DESCRIPTION as LEFT_DESCRIPTION"
).crossJoin(first_3.selectExpr("CODE as RIGHT", "DESCRIPTION as RIGHT_DESCRIPTION"))

result_1 = cross_join.withColumn(
    "SUBSUMES",
    subsumes(
        to_coding(cross_join.LEFT, "http://snomed.info/sct"),
        to_coding(cross_join.RIGHT, "http://snomed.info/sct"),
    ),
)
result_2 = result_1.withColumn(
    "LEFT_IS_ENT",
    subsumes(
        # 232208008 |Ear, nose and throat disorder|
        Coding("http://snomed.info/sct", "232208008"),
        to_coding(cross_join.LEFT, "http://snomed.info/sct"),
    ),
)

result_2.select(
    "LEFT", "RIGHT", "LEFT_DESCRIPTION", "RIGHT_DESCRIPTION", "SUBSUMES", "LEFT_IS_ENT"
).show()
