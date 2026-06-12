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

"""Integration tests for the ``pathling view`` command.

Author: John Grimes.
"""

import csv
import io
import json
import os

import pytest
from pytest import fixture

from pathling.cli.main import cli

PATIENT_COUNT = 9
FIRST_FAMILY = "Krajcik437"

PATIENT_VIEW = {
    "resource": "Patient",
    "select": [
        {
            "column": [
                {"path": "id", "name": "id"},
                {"path": "name.first().family", "name": "family_name"},
            ]
        }
    ],
}


@fixture(scope="module")
def ndjson_source(test_data_dir):
    """The shared ndjson test data directory."""
    return os.path.join(test_data_dir, "ndjson")


@fixture
def view_file(tmp_path):
    """Writes the patient ViewDefinition to a file and returns its path."""
    path = tmp_path / "patient_view.json"
    path.write_text(json.dumps(PATIENT_VIEW), encoding="utf-8")
    return path


# ========== Output formats ==========


def test_view_table_default(runner, patched_context, ndjson_source, view_file):
    """The default output is a formatted table on stdout."""
    result = runner.invoke(cli, ["view", ndjson_source, "--view", str(view_file)])

    assert result.exit_code == 0, result.stderr
    assert FIRST_FAMILY in result.stdout
    assert "family_name" in result.stdout


def test_view_csv(runner, patched_context, ndjson_source, view_file):
    """CSV output has a header row and one row per patient."""
    result = runner.invoke(
        cli, ["view", ndjson_source, "--view", str(view_file), "--format", "csv"]
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows[0] == ["id", "family_name"]
    assert len(rows) == PATIENT_COUNT + 1


def test_view_json(runner, patched_context, ndjson_source, view_file):
    """JSON output is an array of records with the view's columns."""
    result = runner.invoke(
        cli, ["view", ndjson_source, "--view", str(view_file), "--format", "json"]
    )

    assert result.exit_code == 0, result.stderr
    records = json.loads(result.stdout)
    assert len(records) == PATIENT_COUNT
    assert "family_name" in records[0]


def test_view_ndjson(runner, patched_context, ndjson_source, view_file):
    """NDJSON output is one JSON object per line."""
    result = runner.invoke(
        cli, ["view", ndjson_source, "--view", str(view_file), "--format", "ndjson"]
    )

    assert result.exit_code == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == PATIENT_COUNT
    assert "family_name" in json.loads(lines[0])


# ========== File output ==========


def test_view_to_csv_file(runner, patched_context, ndjson_source, view_file, tmp_path):
    """Writing to a CSV file reports the row count on stderr."""
    out = tmp_path / "results.csv"

    result = runner.invoke(
        cli, ["view", ndjson_source, "--view", str(view_file), "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    assert out.exists()
    assert str(PATIENT_COUNT) in result.stderr


def test_view_to_parquet_file(
    runner, patched_context, ndjson_source, view_file, tmp_path
):
    """Writing to a Parquet file creates the file and confirms the count."""
    out = tmp_path / "results.parquet"

    result = runner.invoke(
        cli, ["view", ndjson_source, "--view", str(view_file), "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    assert out.exists()
    assert "Wrote" in result.stderr


@pytest.mark.parametrize(
    "filename,fmt",
    [("results.json", None), ("results.ndjson", None), ("results.delta", "delta")],
)
def test_view_to_other_file_formats(
    runner, patched_context, ndjson_source, view_file, tmp_path, filename, fmt
):
    """Writing to JSON, NDJSON, and Delta file outputs all succeed."""
    out = tmp_path / filename
    args = ["view", ndjson_source, "--view", str(view_file), "-o", str(out)]
    if fmt:
        args += ["--format", fmt]

    result = runner.invoke(cli, args)

    assert result.exit_code == 0, result.stderr
    assert out.exists()


# ========== Inline view ==========


def test_view_json_inline(runner, patched_context, ndjson_source):
    """An inline --view-json string runs the view."""
    result = runner.invoke(
        cli,
        [
            "view",
            ndjson_source,
            "--view-json",
            json.dumps(PATIENT_VIEW),
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "family_name" in result.stdout


# ========== Error paths ==========


def test_malformed_view_json(runner, patched_context, ndjson_source):
    """A malformed ViewDefinition JSON string identifies the problem."""
    result = runner.invoke(
        cli, ["view", ndjson_source, "--view-json", '{"resource": "Patient", ']
    )

    assert result.exit_code == 1
    assert "JSON" in result.stderr


def test_unknown_resource_type(runner, patched_context, ndjson_source):
    """An unknown resource type surfaces an error and exits non-zero."""
    bad_view = json.dumps(
        {
            "resource": "NotARealResource",
            "select": [{"column": [{"path": "id", "name": "id"}]}],
        }
    )
    result = runner.invoke(cli, ["view", ndjson_source, "--view-json", bad_view])

    assert result.exit_code == 1
    assert result.stderr.strip() != ""


def test_view_filter_restricts_rows(runner, patched_context, ndjson_source, view_file):
    """A --filter search expression restricts the rows processed."""
    result = runner.invoke(
        cli,
        [
            "view",
            ndjson_source,
            "--view",
            str(view_file),
            "--filter",
            f"family={FIRST_FAMILY}",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout)))
    # Header plus the matching patients only (fewer than the full set).
    assert 1 < len(rows) <= PATIENT_COUNT
    assert all(row[1] == FIRST_FAMILY for row in rows[1:])


def test_view_empty_result_prints_zero_rows(
    runner, patched_context, ndjson_source, view_file
):
    """An empty result prints an explicit '0 rows' and exits 0."""
    result = runner.invoke(
        cli,
        [
            "view",
            ndjson_source,
            "--view",
            str(view_file),
            "--filter",
            "family=NoSuchFamilyNameExists",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "0 rows" in result.stdout
