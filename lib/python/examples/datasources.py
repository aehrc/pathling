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

from pathling import PathlingContext, exp
from pathling.datasource import DataSources
from pathling.query import AggregateQuery

HERE = os.path.abspath(os.path.dirname(__file__))
PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)

TEST_DATA_DIR = os.path.join(
    PROJECT_DIR, "fhir-server", "src", "test", "resources", "test-data"
)
ndjson_dir = os.path.join(TEST_DATA_DIR, "fhir")
warehouse_dir_uri = "file://" + TEST_DATA_DIR

pc = PathlingContext.create(enable_delta=True)

# Read each line from the NDJSON into a row within a Spark data set.
json_resources = pc.spark.read.text(ndjson_dir, pathGlobFilter="*.ndjson")

#
# Pythonic API for accessing data sources
#

patient_count_by_gender_and_status = agg_result = AggregateQuery(
    "Patient",
    aggregations=[exp("count()").alias("countOfPatients")],
    groupings=["gender", "maritalStatus.coding"],
    filters=["birthDate > @1957-06-06"],
)

#
# Transient ad-hoc data source
#

print("Transient ad-hoc data source")
patient_count_by_gender_and_status.execute(
    pc.data_source.with_resources(
        {
            "Patient": pc.encode(json_resources, "Patient"),
            "Condition": pc.encode(json_resources, "Condition"),
        }
    )
).show(10)

#
# Normalized NDJSON directory data source
#

print("Normalized NDJSON directory data source")

patient_count_by_gender_and_status.execute(
    pc.data_source.from_ndjson_dir(ndjson_dir)
).show(10)

#
#  Text files data source
#

print("Text files data source")

patient_count_by_gender_and_status.execute(
    pc.data_source.from_text_files(
        os.path.join("file://" + ndjson_dir, "*.ndjson"),
        lambda filename: [os.path.basename(filename).split(".")[0]],
    )
).show(10)

#
# Delta warehouse data source
#

print("Delta warehouse data source")

patient_count_by_gender_and_status.execute(
    DataSources(pc).from_warehouse(warehouse_dir_uri, "parquet")
).show(10)
