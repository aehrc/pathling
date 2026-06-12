#
# Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Integration tests for the ``pathling fhirpath`` command.

Author: John Grimes.
"""

import csv
import io
import json
import os

from pytest import fixture

from pathling.cli.main import cli

PATIENT_JSON = {
    "resourceType": "Patient",
    "id": "example",
    "active": True,
    "name": [
        {"use": "official", "family": "Smith", "given": ["John", "James"]},
        {"use": "nickname", "family": "Smith", "given": ["Johnny"]},
    ],
}


@fixture(scope="module")
def ndjson_source(test_data_dir):
    """The shared ndjson test data directory."""
    return os.path.join(test_data_dir, "ndjson")


@fixture
def patient_file(tmp_path):
    """Writes a single Patient resource file and returns its path."""
    path = tmp_path / "patient.json"
    path.write_text(json.dumps(PATIENT_JSON), encoding="utf-8")
    return path


# ========== Data source mode ==========


def test_data_source_mode(runner, patched_context, ndjson_source):
    """Data source mode prints a table of resource IDs and results."""
    result = runner.invoke(
        cli,
        [
            "fhirpath",
            ndjson_source,
            "-t",
            "Patient",
            "-e",
            "name.first().family",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows[0] == ["id", "result"]
    # One row per patient (nine in the fixture).
    assert len(rows) == 10


def test_data_source_mode_requires_type(runner, patched_context, ndjson_source):
    """Omitting -t in data source mode is a usage error."""
    result = runner.invoke(cli, ["fhirpath", ndjson_source, "-e", "name.family"])

    assert result.exit_code == 2
    assert "-t" in result.stderr or "type" in result.stderr.lower()


def test_data_source_filter(runner, patched_context, ndjson_source):
    """A --filter restricts the resources evaluated."""
    result = runner.invoke(
        cli,
        [
            "fhirpath",
            ndjson_source,
            "-t",
            "Patient",
            "-e",
            "name.first().family",
            "--filter",
            "family=Krajcik437",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert 1 < len(rows) < 10


# ========== Single resource mode ==========


def test_single_resource_mode(runner, patched_context, patient_file):
    """Single-resource mode returns typed result values."""
    result = runner.invoke(
        cli,
        ["fhirpath", str(patient_file), "-e", "name.given.first()", "--format", "csv"],
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows[0] == ["type", "value"]
    values = [row[1] for row in rows[1:]]
    assert "John" in values


def test_single_resource_context_and_vars(runner, patched_context, patient_file):
    """A context expression and named variables are honoured."""
    result = runner.invoke(
        cli,
        [
            "fhirpath",
            str(patient_file),
            "-e",
            "%greeting",
            "--var",
            "greeting=hello",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "hello" in result.stdout

    contextual = runner.invoke(
        cli,
        [
            "fhirpath",
            str(patient_file),
            "-e",
            "given",
            "--context",
            "name",
            "--format",
            "csv",
        ],
    )
    assert contextual.exit_code == 0, contextual.stderr
    assert "John" in contextual.stdout


# ========== Error handling ==========


def test_invalid_expression(runner, patched_context, ndjson_source):
    """An invalid expression yields a clear message, exit 1, no stack trace."""
    result = runner.invoke(
        cli, ["fhirpath", ndjson_source, "-t", "Patient", "-e", "name.bogus("]
    )

    assert result.exit_code == 1
    assert result.stderr.strip() != ""
    assert "Traceback" not in result.stderr
    assert "\tat " not in result.stderr


def test_invalid_var_is_usage_error(runner, patched_context, patient_file):
    """A malformed --var is a usage error."""
    result = runner.invoke(
        cli, ["fhirpath", str(patient_file), "-e", "active", "--var", "noequals"]
    )

    assert result.exit_code == 2


def test_context_rejected_in_data_source_mode(runner, patched_context, ndjson_source):
    """--context is rejected in data source mode rather than silently ignored."""
    result = runner.invoke(
        cli,
        [
            "fhirpath",
            ndjson_source,
            "-t",
            "Patient",
            "-e",
            "name.family",
            "--context",
            "name",
        ],
    )

    assert result.exit_code == 2
    assert "single-resource" in result.stderr.lower()
