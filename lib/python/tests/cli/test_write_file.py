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

"""Tests for file output writing in the command line interface.

These pin the on-disk shape and content produced by
:func:`pathling.cli.render._write_file` and :func:`write_output` for each file
format. Output is produced by Spark's native writers and then departitioned to
a single file at the requested path; Delta is written as a Spark table
directory. A Spark session is required to build and write the result
DataFrames, so these tests build small frames from the shared session fixture.

Author: John Grimes.
"""

import csv
import io
import json
from io import StringIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest import fixture
from rich.console import Console

from pathling.cli.errors import CliError
from pathling.cli.render import OutputFormat, OutputSpec, _write_file, write_output


@fixture(scope="module")
def spark(pathling_ctx):
    """Provides the Spark session from the shared Pathling context.

    :param pathling_ctx: the shared Pathling context fixture.
    :return: the underlying Spark session.
    """
    return pathling_ctx.spark


def _spec(
    path: Path, fmt: str, *, overwrite: bool = False, departition: bool = True
) -> OutputSpec:
    """Builds an :class:`OutputSpec` for a file path and format.

    :param path: the output file path.
    :param fmt: the output format.
    :param overwrite: whether an existing target may be replaced.
    :param departition: whether single-file departitioning is applied.
    :return: the output specification.
    """
    return OutputSpec(
        path=path, format=fmt, overwrite=overwrite, departition=departition
    )


def _capture_console():
    """Builds a Rich console that captures its output to a buffer.

    :return: a tuple of the console and the buffer it writes to.
    """
    buffer = StringIO()
    # A wide console keeps long paths on one line so the captured message is
    # asserted on its content rather than on Rich's display-width wrapping.
    return Console(file=buffer, width=10_000, markup=False, highlight=False), buffer


def _assert_only_target(target: Path, parent: Path) -> None:
    """Asserts the target is the sole entry beside it - a true single file.

    No Spark part files, ``_SUCCESS`` or ``.crc`` markers, and no leftover
    departition temporary directory remain beside the target.

    :param target: the expected single output file.
    :param parent: the directory that should contain only the target.
    """
    assert target.is_file()
    leftovers = sorted(entry.name for entry in parent.iterdir() if entry != target)
    assert leftovers == [], f"unexpected entries beside the target: {leftovers}"


# ========== CSV single-file output (T004) ==========


def test_csv_yields_single_file_with_header(spark, tmp_path):
    """``-o out.csv`` is one CSV file with a header and one line per row."""
    df = spark.createDataFrame(
        [("1", "Smith"), ("2", None)], "id string, family string"
    )
    out = tmp_path / "out.csv"

    _write_file(df, _spec(out, OutputFormat.CSV))

    _assert_only_target(out, tmp_path)
    rows = list(csv.reader(io.StringIO(out.read_text())))
    assert rows[0] == ["id", "family"]
    # Compared as a set so the assertion does not depend on row ordering.
    assert {tuple(row) for row in rows[1:]} == {("1", "Smith"), ("2", "")}


# ========== NDJSON single-file output (T005) ==========


def test_ndjson_yields_single_file_with_null_fields(spark, tmp_path):
    """``-o out.ndjson`` is one object per line, each carrying the same keys."""
    df = spark.createDataFrame(
        [("1", "Smith"), ("2", None)], "id string, family string"
    )
    out = tmp_path / "out.ndjson"

    _write_file(df, _spec(out, OutputFormat.NDJSON))

    _assert_only_target(out, tmp_path)
    lines = out.read_text().splitlines()
    assert len(lines) == 2
    records = [json.loads(line) for line in lines]
    # Null-valued fields are retained, so every record has the same key set.
    assert all(set(record.keys()) == {"id", "family"} for record in records)
    assert {record["family"] for record in records} == {"Smith", None}


# ========== Parquet single-file type fidelity (T006) ==========


def test_parquet_yields_single_file_preserving_types(spark, tmp_path):
    """``-o out.parquet`` preserves struct field names, arrays, and int nulls."""
    df = spark.createDataFrame(
        [(1, ("Perth", "6000"), [10, 20]), (None, ("Sydney", None), None)],
        "id int, addr struct<city:string,postcode:string>, scores array<int>",
    )
    out = tmp_path / "out.parquet"

    _write_file(df, _spec(out, OutputFormat.PARQUET))

    _assert_only_target(out, tmp_path)
    schema = pq.read_table(out).schema
    # A nullable integer column stays an integer (not coerced to float).
    assert pa.types.is_integer(schema.field("id").type)
    # The struct keeps its nested field names.
    assert pa.types.is_struct(schema.field("addr").type)
    assert [field.name for field in schema.field("addr").type] == ["city", "postcode"]
    # The array column stays a list.
    assert pa.types.is_list(schema.field("scores").type)


# ========== Delta directory output (T007) ==========


def test_delta_output_is_a_table_directory(spark, tmp_path):
    """``--format delta`` writes a Delta table directory, not a single file."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.delta"

    _write_file(df, _spec(out, OutputFormat.DELTA))

    assert out.is_dir()
    assert (out / "_delta_log").is_dir()


# ========== Overwrite handling (T008) ==========


def test_existing_target_without_overwrite_errors_and_writes_nothing(spark, tmp_path):
    """An existing target without --overwrite errors and leaves it untouched."""
    out = tmp_path / "out.csv"
    out.write_text("original")
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    console, _ = _capture_console()

    with pytest.raises(CliError):
        write_output(df, _spec(out, OutputFormat.CSV), console)

    assert out.read_text() == "original"


def test_overwrite_replaces_an_existing_file(spark, tmp_path):
    """With --overwrite, an existing file is replaced by the new output."""
    out = tmp_path / "out.csv"
    out.write_text("stale,data\n9,9\n")
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")

    _write_file(df, _spec(out, OutputFormat.CSV, overwrite=True))

    _assert_only_target(out, tmp_path)
    text = out.read_text()
    assert "Smith" in text
    assert "stale" not in text


def test_overwrite_replaces_an_existing_directory(spark, tmp_path):
    """With --overwrite, an existing directory target is replaced by a file."""
    out = tmp_path / "out.csv"
    out.mkdir()
    (out / "part-00000-old-c000.csv").write_text("stale\n")
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")

    _write_file(df, _spec(out, OutputFormat.CSV, overwrite=True))

    _assert_only_target(out, tmp_path)
    assert "Smith" in out.read_text()


# ========== Empty result (T009) ==========


def test_empty_result_csv_is_header_only(spark, tmp_path):
    """An empty result still yields a valid single CSV with only the header."""
    df = spark.createDataFrame([], "id string, family string")
    out = tmp_path / "out.csv"

    _write_file(df, _spec(out, OutputFormat.CSV))

    _assert_only_target(out, tmp_path)
    assert out.read_text().strip() == "id,family"


def test_empty_result_ndjson_is_empty_file(spark, tmp_path):
    """An empty result yields a valid, empty single NDJSON file."""
    df = spark.createDataFrame([], "id string, family string")
    out = tmp_path / "out.ndjson"

    _write_file(df, _spec(out, OutputFormat.NDJSON))

    _assert_only_target(out, tmp_path)
    assert out.read_text() == ""


def test_empty_result_parquet_is_schema_only(spark, tmp_path):
    """An empty result yields a schema-only single Parquet file with no rows."""
    df = spark.createDataFrame([], "id string, family string")
    out = tmp_path / "out.parquet"

    _write_file(df, _spec(out, OutputFormat.PARQUET))

    _assert_only_target(out, tmp_path)
    table = pq.read_table(out)
    assert table.num_rows == 0
    assert table.schema.names == ["id", "family"]


# ========== Opt out of departitioning (T013) ==========


@pytest.mark.parametrize(
    "fmt,extension",
    [
        (OutputFormat.CSV, "csv"),
        (OutputFormat.NDJSON, "ndjson"),
        (OutputFormat.PARQUET, "parquet"),
    ],
)
def test_no_departition_yields_a_directory_of_part_files(
    spark, tmp_path, fmt, extension
):
    """--no-departition writes a Spark directory of part files at the target."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / f"out.{extension}"

    _write_file(df, _spec(out, fmt, departition=False))

    assert out.is_dir()
    part_files = [
        entry.name for entry in out.iterdir() if entry.name.startswith("part-")
    ]
    assert part_files, f"expected part files in {out}, found {list(out.iterdir())}"


def test_departition_default_yields_a_single_file(spark, tmp_path):
    """Without the flag, the default departitions to a single file."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.csv"

    _write_file(df, _spec(out, OutputFormat.CSV))

    assert out.is_file()


def test_no_departition_has_no_effect_on_delta(spark, tmp_path):
    """The departition flag does not change Delta's table-directory output."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.delta"

    _write_file(df, _spec(out, OutputFormat.DELTA, departition=False))

    assert out.is_dir()
    assert (out / "_delta_log").is_dir()


# ========== Cleanup and confirmation (T010) ==========


def test_temp_directory_removed_on_write_failure(spark, tmp_path, monkeypatch):
    """A failure during departition leaves no temporary directory behind."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.csv"

    # Simulate a failure after the Spark write has produced the temp directory.
    def _boom(*args, **kwargs):
        raise RuntimeError("departition failed")

    monkeypatch.setattr("pathling.cli.render.departition", _boom)

    with pytest.raises(RuntimeError):
        _write_file(df, _spec(out, OutputFormat.CSV))

    # No target file and no leftover temporary directory beside it.
    assert not out.exists()
    assert list(tmp_path.iterdir()) == []


def test_confirmation_names_format_and_path_without_row_count(spark, tmp_path):
    """The success confirmation names the format and path, with no row count."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.csv"
    console, buffer = _capture_console()

    write_output(df, _spec(out, OutputFormat.CSV), console)

    message = buffer.getvalue()
    assert message.strip() == f"Wrote csv output to {out}."
    assert "row" not in message.lower()
