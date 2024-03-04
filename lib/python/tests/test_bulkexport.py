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
import os

from datetime import timezone, timedelta, datetime
from typing import Optional

import pytest
from py4j.java_gateway import JVMView
from pyspark import SparkContext

from pathling.bulkexport import BulkExportClient, BulkExportResult
from pathling.bulkexport import ExportLevel
from pathling.fhir import Reference

from flask import Response


def test_builds_default_client(pathling_ctx):
    client = BulkExportClient("http://example.com", "output")
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://example.com"
    assert j_client.getOutputDir() == "output"
    assert j_client.getOperation().getPath() == "$export"  # system level export
    assert j_client.getOutputFormat() == "application/fhir+ndjson"
    assert j_client.getSince() is None
    assert j_client.getTimeout().isZero()
    assert list(j_client.getTypes()) == []
    assert list(j_client.getElements()) == []
    assert list(j_client.getTypeFilters()) == []
    assert list(j_client.getPatients()) == []
    assert j_client.getMaxConcurrentDownloads() == 10


def test_builds_system_client_with_options(pathling_ctx):
    client = BulkExportClient(
        "http://system.com",
        "output-system",
        level=ExportLevel.SYSTEM,
        output_format="fhir+ndjson",
        types=["Patient", "Condition"],
        elements=["id", "status"],
        type_filters=["Patient?active=true", "Condition?status=active"],
        since=datetime.fromtimestamp(10000, tz=timezone.utc),
        timeout=timedelta(minutes=2),
        max_concurrent_downloads=3,
    )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://system.com"
    assert j_client.getOutputDir() == "output-system"
    assert j_client.getOperation().getPath() == "$export"  # system level export
    assert j_client.getOutputFormat() == "fhir+ndjson"
    assert j_client.getSince().getEpochSecond() == 10000
    assert j_client.getTimeout().toSeconds() == 120
    assert list(j_client.getTypes()) == ["Patient", "Condition"]
    assert list(j_client.getElements()) == ["id", "status"]
    assert list(j_client.getTypeFilters()) == [
        "Patient?active=true",
        "Condition?status=active",
    ]
    assert list(j_client.getPatients()) == []
    assert j_client.getMaxConcurrentDownloads() == 3


def test_builds_patient_client_with_options(pathling_ctx):
    client = BulkExportClient(
        "http://patient.com",
        "output-patient",
        level=ExportLevel.PATIENT,
        types=["Observation", "Condition"],
        since=datetime.fromtimestamp(2000, tz=timezone.utc),
        patients=[Reference("Patient/123"), Reference("Patient/456")],
        timeout=timedelta(seconds=2),
    )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://patient.com"
    assert j_client.getOutputDir() == "output-patient"
    assert (
        j_client.getOperation().getPath() == "Patient/$export"
    )  # patient level export
    assert j_client.getOutputFormat() == "application/fhir+ndjson"
    assert j_client.getSince().getEpochSecond() == 2000
    assert j_client.getTimeout().toSeconds() == 2
    assert list(j_client.getTypes()) == ["Observation", "Condition"]
    assert [p.getReference() for p in j_client.getPatients()] == [
        "Patient/123",
        "Patient/456",
    ]


def test_builds_group_client_with_options(pathling_ctx):
    client = BulkExportClient(
        "http://group.com", "output-group", level=ExportLevel.GROUP, group_id="123"
    )
    j_client = client._jclient
    assert j_client.getFhirEndpointUrl() == "http://group.com"
    assert j_client.getOutputDir() == "output-group"
    assert (
        j_client.getOperation().getPath() == "Group/123/$export"
    )  # group level export
    assert j_client.getOutputFormat() == "application/fhir+ndjson"
    assert j_client.getSince() is None
    assert j_client.getTimeout().isZero()
    assert list(j_client.getTypes()) == []
    assert list(j_client.getPatients()) == []


def test_raises_error_if_group_without_id(pathling_ctx):
    with pytest.raises(ValueError) as exc_info:
        BulkExportClient("http://group.com", "output-group", level=ExportLevel.GROUP)
    assert exc_info.match("groupId is required for group export")


def test_build_correct_export_result(pathling_ctx):
    jvm: Optional[JVMView] = SparkContext._active_spark_context._jvm
    j_result = jvm.au.csiro.pathling.export.BulkExportResult.of(
        jvm.java.time.Instant.ofEpochMilli(1000_123), []
    )
    result = BulkExportResult.from_java(j_result)
    assert result.transaction_time == datetime.fromtimestamp(1000.123, tz=timezone.utc)


def test_runs_export(pathling_ctx, mock_server, tmp_path):
    @mock_server.route("/fhir/$export", methods=["GET"])
    def export():
        resp = Response(status=202)
        resp.headers["content-location"] = mock_server.url("/pool")
        return resp

    @mock_server.route("/pool", methods=["GET"])
    def pool():
        return dict(
            transactionTime=0,
            output=[
                dict(type="Patient", url=mock_server.url("/download"), count=1),
            ],
        )

    @mock_server.route("/download", methods=["GET"])
    def download():
        return '{"id":"123"}'

    output_dir = os.path.join(tmp_path, "export-output")

    with mock_server.run():
        result = BulkExportClient(mock_server.url("/fhir"), output_dir).export()

        assert os.path.isdir(output_dir)
        assert os.path.exists(os.path.join(output_dir, "_SUCCESS"))
        assert os.path.exists(os.path.join(output_dir, "Patient.0000.ndjson"))
        with open(os.path.join(output_dir, "Patient.0000.ndjson")) as f:
            assert f.read() == '{"id":"123"}'
        assert result.transaction_time == datetime.fromtimestamp(0, tz=timezone.utc)
