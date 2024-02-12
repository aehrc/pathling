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
from pathling.bulkexport import bulk_export
from datetime import datetime, timezone
from time import time

HERE = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(HERE, os.pardir))
TARGET_DIR = os.path.abspath(os.path.join(BASE_DIR, "target"))

# create the context to initialize Java backend for SparkSession
pc = PathlingContext.create(log_level='DEBUG')

# Smart Bulk Data Server https://bulk-data.smarthealthit.org/index.html
# 100 Patients, System level export
fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir"
outputDirUrl = os.path.join(TARGET_DIR, "export-%s" % int(time()))

print(f"Exporting from: {fhirEndpointUrl} to {outputDirUrl}")
bulk_export(
    fhirEndpointUrl,
    outputDirUrl,
    _outputFormat="ndjson",
    _type=["Patient", "Condition"],
    _since=datetime.fromisoformat('2020-01-01T00:00:00').replace(tzinfo=timezone.utc)
)
