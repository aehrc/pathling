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
import datetime

from datetime import timezone, timedelta, datetime
from typing import Optional

import pytest
from py4j.java_gateway import JVMView
from pyspark import SparkContext

from pathling.bulkexport import BulkExportClient, BulkExportResult
from pathling.bulkexport import Method
from pathling.fhir import Reference


def test_builds_default_client(pathling_ctx):
    client = BulkExportClient("http://example.com", "output")
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://example.com"
    assert j_client.getOutputDir() == "output"
    assert j_client.getOperation().getPath() == "$export"  # system level export
    assert j_client.getOutputFormat() == "ndjson"
    assert j_client.getSince() is None
    assert j_client.getTimeout().isZero()
    assert list(j_client.getTypes()) == []
    assert list(j_client.getPatients()) == []


def test_builds_system_client_with_options(pathling_ctx):
    client = BulkExportClient("http://system.com", "output-system",
                              method=Method.SYSTEM,
                              outputFormat="fhir+ndjson",
                              types=['Patient', 'Condition'],
                              since=datetime.fromtimestamp(10000, tz=timezone.utc),
                              timeout=timedelta(minutes=2)
                              )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://system.com"
    assert j_client.getOutputDir() == "output-system"
    assert j_client.getOperation().getPath() == "$export"  # system level export
    assert j_client.getOutputFormat() == "fhir+ndjson"
    assert j_client.getSince().getEpochSecond() == 10000
    assert j_client.getTimeout().toSeconds() == 120
    assert list(j_client.getTypes()) == ['Patient', 'Condition']
    assert list(j_client.getPatients()) == []


def test_builds_patient_client_with_options(pathling_ctx):
    client = BulkExportClient("http://patient.com", "output-patient",
                              method=Method.PATIENT,
                              types=['Observation', 'Condition'],
                              patients=[Reference('Patient/123'), Reference('Patient/456')],
                              since=datetime.fromtimestamp(2000, tz=timezone.utc),
                              timeout=timedelta(seconds=2)
                              )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://patient.com"
    assert j_client.getOutputDir() == "output-patient"
    assert j_client.getOperation().getPath() == "Patient/$export"  # patient level export
    assert j_client.getOutputFormat() == "ndjson"
    assert j_client.getSince().getEpochSecond() == 2000
    assert j_client.getTimeout().toSeconds() == 2
    assert list(j_client.getTypes()) == ['Observation', 'Condition']
    assert [p.getReference() for p in j_client.getPatients()] == ['Patient/123', 'Patient/456']


def test_builds_group_client_with_options(pathling_ctx):
    client = BulkExportClient("http://group.com", "output-group",
                              method=Method.GROUP,
                              groupId="123",
                              )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://group.com"
    assert j_client.getOutputDir() == "output-group"
    assert j_client.getOperation().getPath() == "Group/123/$export"  # group level export
    assert j_client.getOutputFormat() == "ndjson"
    assert j_client.getSince() is None
    assert j_client.getTimeout().isZero()
    assert list(j_client.getTypes()) == []
    assert list(j_client.getPatients()) == []


def test_raises_error_if_group_without_id(pathling_ctx):
    with pytest.raises(ValueError) as exc_info:
        BulkExportClient("http://group.com", "output-group",
                         method=Method.GROUP,
                         )
    assert exc_info.match("groupId is required for group export")


def test_build_correct_export_result(pathling_ctx):
    jvm: Optional[JVMView] = SparkContext._active_spark_context._jvm
    j_result = jvm.au.csiro.pathling.export.BulkExportResult.of(
        jvm.java.time.Instant.ofEpochMilli(1000_123), [])
    result = BulkExportResult._from_java(j_result)
    assert result.transactionTime == datetime.fromtimestamp(1000.123, tz=timezone.utc)
