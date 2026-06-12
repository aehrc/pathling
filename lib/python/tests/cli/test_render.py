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

"""Unit tests for output rendering and output option resolution.

The pure rendering and validation logic is tested without Spark.

Author: John Grimes.
"""

import csv
import io
import json

import pytest

from pathling.cli.errors import CliError
from pathling.cli.render import (
    OutputFormat,
    check_overwrite,
    infer_format_from_extension,
    render_csv,
    render_json,
    render_ndjson,
    render_table,
    resolve_output,
)

COLUMNS = ["id", "family"]
ROWS = [["1", "Smith"], ["2", None]]


# ========== Table ==========


def test_table_includes_values_and_row_count():
    """The table renders values and a row-count caption."""
    output = render_table(COLUMNS, ROWS)

    assert "Smith" in output
    assert "2 rows" in output


def test_table_empty_indicates_zero_rows():
    """An empty result renders an explicit '0 rows' indication."""
    output = render_table(COLUMNS, [])

    assert "0 rows" in output


def test_table_renders_square_brackets_verbatim():
    """Cell values containing square brackets are not treated as Rich markup."""
    # A value like a Spark error code would crash a markup-enabled renderer.
    output = render_table(["v"], [["[FAILED_EXECUTE_UDF] detail"]])

    assert "[FAILED_EXECUTE_UDF]" in output


# ========== CSV ==========


def test_csv_has_header_and_rows():
    """CSV output includes a header row and parses back to the input rows."""
    output = render_csv(COLUMNS, ROWS)

    parsed = list(csv.reader(io.StringIO(output)))
    assert parsed[0] == COLUMNS
    assert parsed[1] == ["1", "Smith"]
    # None is rendered as an empty field.
    assert parsed[2] == ["2", ""]


# ========== JSON ==========


def test_json_is_array_of_objects():
    """JSON output is a valid array of column-keyed objects."""
    output = render_json(COLUMNS, ROWS)

    parsed = json.loads(output)
    assert parsed == [
        {"id": "1", "family": "Smith"},
        {"id": "2", "family": None},
    ]


# ========== NDJSON ==========


def test_ndjson_is_one_object_per_line():
    """NDJSON output is one JSON object per line."""
    output = render_ndjson(COLUMNS, ROWS)

    lines = output.splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": "1", "family": "Smith"}


# ========== Format inference ==========


def test_infer_format_from_extension():
    """Output format is inferred from the file extension."""
    from pathlib import Path

    assert infer_format_from_extension(Path("out.csv")) == OutputFormat.CSV
    assert infer_format_from_extension(Path("out.json")) == OutputFormat.JSON
    assert infer_format_from_extension(Path("out.ndjson")) == OutputFormat.NDJSON
    assert infer_format_from_extension(Path("out.parquet")) == OutputFormat.PARQUET
    assert infer_format_from_extension(Path("out.unknown")) is None


def test_resolve_output_infers_from_path():
    """An output path with a known extension resolves its format."""
    spec = resolve_output("results.csv", None)

    assert spec.format == OutputFormat.CSV
    assert str(spec.path) == "results.csv"


def test_resolve_output_default_is_table():
    """With no path and no flag, the default format is a table."""
    spec = resolve_output(None, None)

    assert spec.format == OutputFormat.TABLE
    assert spec.path is None


# ========== Validation errors ==========


def test_table_with_output_path_is_error():
    """Requesting the table format with -o is a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_output("out.txt", OutputFormat.TABLE)

    assert exc_info.value.exit_code == 2


def test_parquet_without_output_path_is_error():
    """Requesting parquet without -o is a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_output(None, OutputFormat.PARQUET)

    assert exc_info.value.exit_code == 2


def test_unknown_extension_without_format_is_error():
    """An unknown -o extension without --format is a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_output("out.weird", None)

    assert exc_info.value.exit_code == 2


# ========== Overwrite handling ==========


def test_check_overwrite_existing_without_flag_errors(tmp_path):
    """An existing output path without --overwrite is an error showing the flag."""
    existing = tmp_path / "out.csv"
    existing.write_text("data")

    with pytest.raises(CliError) as exc_info:
        check_overwrite(existing, overwrite=False)

    assert "--overwrite" in exc_info.value.message


def test_check_overwrite_existing_with_flag_ok(tmp_path):
    """An existing output path with --overwrite is allowed."""
    existing = tmp_path / "out.csv"
    existing.write_text("data")

    check_overwrite(existing, overwrite=True)


def test_check_overwrite_missing_ok(tmp_path):
    """A non-existent output path passes the overwrite check."""
    check_overwrite(tmp_path / "new.csv", overwrite=False)
