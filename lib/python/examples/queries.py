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

from pathling import PathlingContext
from pathling.query import exp

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = os.path.join(HERE, "data/resources/")
json_resources = pc.spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.

data_source = (
    pc.client_builder()
    .with_resource("Patient", pc.encode(json_resources, "Patient"))
    .with_resource("Condition", pc.encode(json_resources, "Condition"))
    .build()
)

#
# Fluent API
#

result = (
    data_source.extract_query("Patient")
    .with_column("id")
    .with_column("gender")
    .with_column("reverseResolve(Condition.subject).code.coding.code")
    .with_filter("gender = 'male'")
    .execute()
)

result.limit(10).show()

agg_result = (
    data_source.aggregate_query("Patient")
    .with_aggregation("count()", "countOfPatients")
    .with_grouping("gender")
    .with_grouping("maritalStatus.coding")
    .with_filter("birthDate > @1957-06-06")
    .execute()
)

agg_result.show(10)

#
# "Pythonic" API
#

result = data_source.extract(
    "Patient",
    columns=[
        "id",
        "gender",
        ("reverseResolve(Condition.subject).code.coding.code", "condition_code"),
    ],
    filters=["gender = 'male'"],
)

result.limit(10).show()

agg_result = data_source.aggregate(
    "Patient",
    aggregations=[exp("count()").alias("countOfPatients")],
    groupings=["gender", "maritalStatus.coding"],
    filters=["birthDate > @1957-06-06"],
)

agg_result.show(10)
