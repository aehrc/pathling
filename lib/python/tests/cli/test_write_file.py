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

"""Tests for single-file output writing in the command line interface.

These tests pin the on-disk content and column types produced by
:func:`pathling.cli.render._write_file` for each file format, and verify the
reported row count. Parquet is written via Arrow to preserve column types
faithfully; CSV, JSON, and NDJSON are written via pandas. A Spark session is
required to build the result DataFrames, so these tests build small frames
directly from the shared session fixture.

Author: John Grimes.
"""

import csv
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pytest import fixture

from pathling.cli.render import OutputFormat, OutputSpec, _write_file


@fixture(scope="module")
def spark(pathling_ctx):
    """Provides the Spark session from the shared Pathling context.

    :param pathling_ctx: the shared Pathling context fixture.
    :return: the underlying Spark session.
    """
    return pathling_ctx.spark


def _spec(path: Path, fmt: str) -> OutputSpec:
    """Builds an :class:`OutputSpec` for a file path and format.

    :param path: the output file path.
    :param fmt: the output format.
    :return: the output specification.
    """
    return OutputSpec(path=path, format=fmt)


# ========== Parquet column-type fidelity ==========


def test_parquet_preserves_struct_column_field_names(spark, tmp_path):
    """Struct columns keep their nested field names through Parquet output.

    A pandas round-trip degrades a Spark struct to an anonymous list, dropping
    the field names; the Arrow-based writer preserves the struct schema.
    """
    df = spark.createDataFrame(
        [(1, ("Perth", "6000")), (2, ("Sydney", None))],
        "id int, addr struct<city:string,postcode:string>",
    )
    out = tmp_path / "out.parquet"

    count = _write_file(df, _spec(out, OutputFormat.PARQUET))

    table = pq.read_table(out)
    # The column remains a struct rather than collapsing to a list, and its
    # round-tripped values carry the original field names.
    assert pa.types.is_struct(table.schema.field("addr").type)
    assert table.to_pylist() == [
        {"id": 1, "addr": {"city": "Perth", "postcode": "6000"}},
        {"id": 2, "addr": {"city": "Sydney", "postcode": None}},
    ]
    assert count == 2


def test_parquet_preserves_integer_type_with_nulls(spark, tmp_path):
    """An integer column containing nulls stays an integer type in Parquet.

    A pandas round-trip coerces nullable integers to floating point; the
    Arrow-based writer keeps the integer type.
    """
    df = spark.createDataFrame([(1,), (None,)], "count int")
    out = tmp_path / "out.parquet"

    _write_file(df, _spec(out, OutputFormat.PARQUET))

    assert pa.types.is_integer(pq.read_table(out).schema.field("count").type)


# ========== CSV / JSON / NDJSON content ==========


def test_csv_writes_header_values_and_empty_for_null(spark, tmp_path):
    """CSV output has a header, the row values, and empty fields for nulls."""
    df = spark.createDataFrame(
        [("1", "Smith"), ("2", None)], "id string, family string"
    )
    out = tmp_path / "out.csv"

    count = _write_file(df, _spec(out, OutputFormat.CSV))

    rows = list(csv.reader(io.StringIO(out.read_text())))
    assert rows[0] == ["id", "family"]
    # Compared as a set so the assertion does not depend on row ordering.
    assert {tuple(row) for row in rows[1:]} == {("1", "Smith"), ("2", "")}
    assert count == 2


def test_json_file_is_pretty_array_of_records(spark, tmp_path):
    """JSON output is a multi-line array of column-keyed objects."""
    df = spark.createDataFrame([("1", "Smith")], "id string, family string")
    out = tmp_path / "out.json"

    count = _write_file(df, _spec(out, OutputFormat.JSON))

    text = out.read_text()
    assert json.loads(text) == [{"id": "1", "family": "Smith"}]
    # Indented output spans multiple lines rather than a single dense line.
    assert "\n" in text
    assert count == 1


def test_ndjson_file_is_one_object_per_line(spark, tmp_path):
    """NDJSON output is one JSON object per line."""
    df = spark.createDataFrame(
        [("1", "Smith"), ("2", "Jones")], "id string, family string"
    )
    out = tmp_path / "out.ndjson"

    count = _write_file(df, _spec(out, OutputFormat.NDJSON))

    lines = out.read_text().splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert {record["family"] for record in parsed} == {"Smith", "Jones"}
    assert count == 2
