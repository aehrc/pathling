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

from pathling import (
    PathlingContext,
    to_snomed_coding,
    to_ecl_value_set,
    member_of,
)

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create(
    terminology_server_url="http://localhost:8081/fhir",
    terminology_verbose_request_logging=True,
    cache_override_expiry=2_628_000,
    cache_storage_type="disk",
    cache_storage_path=".local/tx-cache",
)
pc.spark.sparkContext.setLogLevel("DEBUG")

csv = pc.spark.read.options(header=True).csv(
    f'file://{os.path.join(HERE, "data/csv/conditions.csv")}'
)

VIRAL_INFECTION_ECL = """
    << 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )
"""

csv.select(
    "CODE",
    "DESCRIPTION",
    member_of(to_snomed_coding(csv.CODE), to_ecl_value_set(VIRAL_INFECTION_ECL)).alias(
        "VIRAL_INFECTION"
    ),
).show()
