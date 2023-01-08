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

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = os.path.join(HERE, "data/resources/")
json_resources = pc.spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode(json_resources, "Patient")

# Do some stuff.
patients.select("id", "gender", "birthDate").show()
