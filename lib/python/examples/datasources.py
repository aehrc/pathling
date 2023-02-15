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

from pathling import PathlingContext, MimeType
from pathling.context import DataSources

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = os.path.join(HERE, "data/resources/")
json_resources = pc.spark.read.text(ndjson_dir)

#
# Pythonic API for accessing data sources
#

# Direct resources
DataSources(pc).with_resources(
  {
    "Patient": pc.encode(json_resources, "Patient"),
    "Condition": pc.encode(json_resources, "Condition"),
  }
).extract("Patient", columns=["id", "gender", "name.given"]).show(10)

# From text encoded resources
DataSources(pc).from_text_resources(
  json_resources, ["Patient", "Condition"], input_type=MimeType.FHIR_JSON
).extract("Patient", columns=["id", "gender", "name.given"]).show(10)

# From json json directory

pc.datasource.from_ndjson(ndjson_dir).extract(
  "Patient", columns=["id", "gender", "name.given"]
).show(10)

# there will be also an option to create a datasource from the encoded delta directory, something like
#
# database = pc.datasource.from_delta_warehouse(delta_dir, ....)
# database.extract(
#     "Patient", columns=["id", "gender", "name.given"]
# ).show(10)

# as well as in it might be possible to write/append/merge a datasource to another (modifiable) one
# to achieve import functionality

# database.import_from(pc.datasource.from_ndjson(ndjson_dir), mode = "append|merge|overwrite", resources = ...)
