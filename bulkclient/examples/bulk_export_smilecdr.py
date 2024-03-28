#!/usr/bin/env python

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
from pathling.bulkexport import BulkExportClient
from datetime import datetime, timezone, timedelta
from time import time

from pathling.fhir import as_fhir_instant, from_fhir_instant
from pathling.config import auth_config

HERE = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(HERE, os.pardir))
TARGET_DIR = os.path.abspath(os.path.join(BASE_DIR, "target"))

# create the context to initialize Java backend for SparkSession
pc = PathlingContext.create(log_level="DEBUG")

fhirEndpointUrl = "https://aehrc-cdr.cc/fhir_r4"
outputDirUrl = os.path.join(TARGET_DIR, "export-%s" % int(time()))
clientSecret = os.getenv('SIMLECDR_CLIENT_SECRET')

print(f"Exporting from: {fhirEndpointUrl} to {outputDirUrl}")
result = BulkExportClient(
    fhirEndpointUrl,
    outputDirUrl,
    types=["Patient", "Condition"],
    type_filters=["Patient: name co \"Bin\""],
    timeout = timedelta(minutes=30),
    auth_config=auth_config(
        auth_enabled=True,
        auth_token_endpoint="https://aehrc-cdr.cc/smartsec_r4/oauth/token",
        auth_client_id="pathling-bulk-client",
        auth_client_secret=clientSecret,
        auth_scope="system/*.read",
    ),
).export()
print(f"TransactionTime: {as_fhir_instant(result.transaction_time)}")
