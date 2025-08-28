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

from pathling import (
    PathlingContext,
    to_snomed_coding,
    property_of,
    display,
    PropertyType,
)

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

csv = pc.spark.read.options(header=True).csv(
    f'file://{os.path.join(HERE, "data/csv/conditions.csv")}'
)

# Get the parent codes for each code in the dataset.
parents = csv.withColumn(
    "PARENTS",
    property_of(to_snomed_coding(csv.CODE), "parent", PropertyType.CODE),
)
# Split each parent code into a separate row.
exploded_parents = parents.selectExpr(
    "CODE", "DESCRIPTION", "explode_outer(PARENTS) AS PARENT"
)
# Retrieve the preferred term for each parent code.
with_displays = exploded_parents.withColumn(
    "PARENT_DISPLAY", display(to_snomed_coding(exploded_parents.PARENT))
)
with_displays.show()
