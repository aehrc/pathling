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

"""Integration tests for the terminology commands.

These run against the JVM mock terminology service wired into the shared
``pathling_ctx`` fixture, using codes whose results are known from the library
test suite.

Author: John Grimes.
"""

import csv
import io
import json

from pytest import fixture, raises

from pathling.cli.main import cli

SNOMED = "http://snomed.info/sct"
LOINC = "http://loinc.org"
VALUE_SET = "http://snomed.info/sct?fhir_vs=refset/723264001"
CONCEPT_MAP = "http://snomed.info/sct?fhir_cm=100"


def _write_csv(path, header, rows):
    """Writes a CSV file with the given header and rows."""
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def _write_parquet(spark, path, header, rows):
    """Writes a Parquet dataset directory with the given header and rows.

    The dataset is written through the shared Spark session, so it takes the
    same directory-of-part-files form as the CLI's own Parquet output. Both the
    single-file and directory forms of Parquet are read back through
    ``spark.read.parquet``, so this directory form exercises the directory case.
    """
    spark.createDataFrame([tuple(row) for row in rows], header).write.parquet(str(path))
    return path


def _write_delta(spark, path, header, rows):
    """Writes a Delta table directory with the given header and rows.

    The table is written through the shared Spark session, which is Delta
    enabled, producing a directory containing a ``_delta_log`` entry alongside
    the Parquet data files.
    """
    spark.createDataFrame([tuple(row) for row in rows], header).write.format(
        "delta"
    ).save(str(path))
    return path


@fixture
def codes_csv(tmp_path):
    """A CSV of SNOMED codes with a system column."""
    return _write_csv(
        tmp_path / "codes.csv",
        ["code", "system"],
        [["368529001", SNOMED], ["439319006", SNOMED]],
    )


def _stdout_rows(result):
    """Parses CSV stdout into a list of rows."""
    return list(csv.reader(io.StringIO(result.stdout)))


# ========== _coding_column ==========


def test_coding_column_uses_fixed_code_literal(pathling_ctx):
    """_coding_column applies a fixed literal code to every row when code= is set."""
    from pathling.cli.terminology import _coding_column

    df = pathling_ctx.spark.createDataFrame([("a",), ("b",)], ["code_col"])
    coding = _coding_column(pathling_ctx, "code_col", SNOMED, None, None, code="FIXED")
    codes = [row[0] for row in df.select(coding.getField("code")).collect()]
    # The per-row code column is ignored in favour of the fixed literal.
    assert codes == ["FIXED", "FIXED"]


def test_coding_column_uses_code_column_when_no_fixed_code(pathling_ctx):
    """_coding_column reads the per-row code column when no fixed code is given."""
    from pathling.cli.terminology import _coding_column

    df = pathling_ctx.spark.createDataFrame([("a",), ("b",)], ["code_col"])
    coding = _coding_column(pathling_ctx, "code_col", SNOMED, None, None)
    codes = [row[0] for row in df.select(coding.getField("code")).collect()]
    # With no fixed code, each row's code is taken from the column.
    assert codes == ["a", "b"]


def test_coding_column_matches_library_schema(pathling_ctx):
    """The CLI Coding column matches the library's Coding schema (field names and
    order), so it cannot drift from the library definition (FR-018)."""
    from pyspark.sql.functions import lit

    from pathling.cli.terminology import _coding_column
    from pathling.functions import to_coding

    # A dataset with a "code" column so the per-row code reference resolves.
    base = pathling_ctx.spark.createDataFrame([("368529001",)], ["code"])
    cli_fields = (
        base.select(_coding_column(pathling_ctx, "code", SNOMED, None, None).alias("c"))
        .schema["c"]
        .dataType.fieldNames()
    )
    library_fields = (
        base.select(to_coding(lit("x"), SNOMED).alias("c"))
        .schema["c"]
        .dataType.fieldNames()
    )

    assert cli_fields == library_fields


# ========== CSV read delimiter (US1) ==========


def test_read_dataset_parses_tab_separated(pathling_ctx, delimited_csv):
    """_read_dataset parses a tab-separated CSV into the correct columns (T010)."""
    from pathling.cli.terminology import _read_dataset

    path = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="tab.csv",
    )

    df = _read_dataset(pathling_ctx, str(path), "csv", delimiter="\t")

    # Without the delimiter the tabbed line would collapse into one column.
    assert df.columns == ["code", "system"]
    assert df.collect()[0]["code"] == "368529001"


def test_read_dataset_parses_semicolon_separated(pathling_ctx, delimited_csv):
    """_read_dataset parses a semicolon-separated CSV into columns (T010)."""
    from pathling.cli.terminology import _read_dataset

    path = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter=";",
        name="semi.csv",
    )

    df = _read_dataset(pathling_ctx, str(path), "csv", delimiter=";")

    assert df.columns == ["code", "system"]
    assert df.collect()[0]["code"] == "368529001"


def test_read_dataset_reads_tsv_extension(pathling_ctx, delimited_csv):
    """_read_dataset reads a .tsv-named file as CSV with the supplied delimiter."""
    from pathling.cli.terminology import _read_dataset

    path = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    df = _read_dataset(pathling_ctx, str(path), "csv", delimiter="\t")

    assert df.columns == ["code", "system"]
    assert df.collect()[0]["code"] == "368529001"


def test_member_of_tsv_round_trip(runner, patched_context, delimited_csv, tmp_path):
    """member-of round-trips a .tsv file to a .tsv output (quickstart P1)."""
    dataset = delimited_csv(
        [["368529001", SNOMED], ["439319006", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )
    out = tmp_path / "out.tsv"

    # No explicit --format: the output format is inferred from the .tsv extension.
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--delimiter",
            "\\t",
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(out.read_text()), delimiter="\t"))
    assert rows[0] == ["code", "system", "member_of"]


def test_member_of_tab_separated_round_trip(
    runner, patched_context, delimited_csv, tmp_path
):
    """member-of round-trips a tab-separated dataset to stdout and a file (T011)."""
    dataset = delimited_csv(
        [["368529001", SNOMED], ["439319006", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes_tab.csv",
    )
    base = [
        "member-of",
        str(dataset),
        "--code-column",
        "code",
        "--system",
        SNOMED,
        "--value-set",
        VALUE_SET,
        "--delimiter",
        "\\t",
    ]

    # Stdout: the input parses correctly and the output is tab-separated.
    result = runner.invoke(cli, base + ["--format", "csv"])
    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(result.stdout), delimiter="\t"))
    assert rows[0] == ["code", "system", "member_of"]
    assert rows[1][2] == "True"

    # File: the written file is tab-separated with the same columns.
    out = tmp_path / "out.csv"
    file_result = runner.invoke(cli, base + ["-o", str(out)])
    assert file_result.exit_code == 0, file_result.stderr
    file_rows = list(csv.reader(io.StringIO(out.read_text()), delimiter="\t"))
    assert file_rows[0] == ["code", "system", "member_of"]


# ========== Extension-derived delimiter inference (T006) ==========


def _member_of_args(dataset):
    """Builds the common member-of argument list for a dataset path.

    :param dataset: the dataset path to read.
    :return: the argument list, ready for further options to be appended.
    """
    return [
        "member-of",
        str(dataset),
        "--code-column",
        "code",
        "--system",
        SNOMED,
        "--value-set",
        VALUE_SET,
    ]


def test_tsv_input_parses_without_a_delimiter_flag(
    runner, patched_context, delimited_csv
):
    """A .tsv input parses into its columns with no --delimiter (FR-002)."""
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    result = runner.invoke(cli, _member_of_args(dataset) + ["--format", "csv"])

    # Without the inference the whole tabbed line would collapse into one
    # column and the --code-column lookup would fail.
    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[0] == ["code", "system", "member_of"]


def test_tsv_input_prints_the_input_side_notice(
    runner, patched_context, delimited_csv, wide_stderr
):
    """The inferred tab on the input side is announced on stderr (FR-007)."""
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    result = runner.invoke(cli, _member_of_args(dataset) + ["--format", "csv"])

    assert result.exit_code == 0, result.stderr
    assert (
        f"Reading {dataset} as tab-separated CSV, inferred from the .tsv extension."
        in result.stderr
    )


def test_csv_input_reads_with_a_comma_and_no_notice(runner, patched_context, codes_csv):
    """A .csv input keeps the comma default and prints no notice (FR-008)."""
    result = runner.invoke(cli, _member_of_args(codes_csv) + ["--format", "csv"])

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[0] == ["code", "system", "member_of"]
    assert "tab-separated" not in result.stderr


def test_explicit_delimiter_overrides_the_extension_on_both_sides(
    runner, patched_context, delimited_csv, tmp_path
):
    """An explicit --delimiter wins over both path extensions (FR-003)."""
    # A semicolon-separated file deliberately named .tsv: the explicit flag must
    # win, otherwise the read would fail to find the code column.
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter=";",
        name="codes.tsv",
    )
    out = tmp_path / "out.tsv"

    result = runner.invoke(
        cli, _member_of_args(dataset) + ["--delimiter", ";", "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    # Both sides used the semicolon; no inference occurred on either.
    rows = list(csv.reader(io.StringIO(out.read_text()), delimiter=";"))
    assert rows[0] == ["code", "system", "member_of"]
    assert "tab-separated" not in result.stderr


def test_explicit_comma_on_a_tsv_path_yields_a_comma(
    runner, patched_context, codes_csv, tmp_path
):
    """An explicit comma on a .tsv output path is honoured with no notice."""
    out = tmp_path / "out.tsv"

    result = runner.invoke(
        cli, _member_of_args(codes_csv) + ["--delimiter", ",", "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(out.read_text())))
    assert rows[0] == ["code", "system", "member_of"]
    assert "tab-separated" not in result.stderr


def test_tsv_path_with_from_parquet_involves_no_delimiter(
    runner, patched_context, tmp_path, pathling_ctx
):
    """A .tsv path read as Parquet uses no delimiter and prints no notice (FR-005)."""
    # A Parquet dataset directory deliberately named .tsv, read with an explicit
    # --from, so the resolved format is not CSV.
    dataset = _write_parquet(
        pathling_ctx.spark,
        tmp_path / "data.tsv",
        "code string, system string",
        [["368529001", SNOMED]],
    )

    result = runner.invoke(
        cli, _member_of_args(dataset) + ["--from", "parquet", "--format", "csv"]
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[0] == ["code", "system", "member_of"]
    assert "tab-separated" not in result.stderr


def test_tsv_path_with_from_csv_still_infers_a_tab(
    runner, patched_context, delimited_csv
):
    """Inference is independent of --from, so an explicit csv still infers a tab.

    The extension governs the delimiter; ``--from`` governs only the format
    (FR-006).
    """
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    result = runner.invoke(
        cli, _member_of_args(dataset) + ["--from", "csv", "--format", "csv"]
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[0] == ["code", "system", "member_of"]
    assert "tab-separated" in result.stderr


def test_zero_flag_tsv_round_trip(
    runner, patched_context, delimited_csv, tmp_path, wide_stderr
):
    """A .tsv in and .tsv out round trip needs no delimiter flag (SC-001).

    This is quickstart Scenario 1: both sides infer a tab from their own path,
    and both announce it.
    """
    dataset = delimited_csv(
        [["368529001", SNOMED], ["439319006", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )
    out = tmp_path / "out.tsv"

    result = runner.invoke(cli, _member_of_args(dataset) + ["-o", str(out)])

    assert result.exit_code == 0, result.stderr
    rows = list(csv.reader(io.StringIO(out.read_text()), delimiter="\t"))
    assert rows[0] == ["code", "system", "member_of"]
    assert len(rows[0]) == 3
    # Both sides announced their inference.
    assert f"Reading {dataset} as tab-separated CSV" in result.stderr
    assert f"Writing {out} as tab-separated CSV" in result.stderr


def test_tsv_in_csv_out_resolves_each_side_independently(
    runner, patched_context, delimited_csv, tmp_path, wide_stderr
):
    """A .tsv input and .csv output resolve separately (quickstart Scenario 2)."""
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )
    out = tmp_path / "out.csv"

    result = runner.invoke(cli, _member_of_args(dataset) + ["-o", str(out)])

    assert result.exit_code == 0, result.stderr
    # The input was read as tab-separated and the output written comma-separated.
    rows = list(csv.reader(io.StringIO(out.read_text())))
    assert rows[0] == ["code", "system", "member_of"]
    # Only the input side announced an inference.
    assert f"Reading {dataset} as tab-separated CSV" in result.stderr
    assert "Writing" not in result.stderr


def test_stdout_stays_comma_separated_for_a_tsv_input(
    runner, patched_context, delimited_csv
):
    """With no -o there is no path to infer from, so stdout is comma-separated."""
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    result = runner.invoke(cli, _member_of_args(dataset) + ["--format", "csv"])

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == "code,system,member_of"


def test_invalid_output_path_fails_without_announcing_a_read(
    runner, patched_context, delimited_csv, tmp_path
):
    """An unusable output path fails before the input-side notice is printed.

    Announcing a read that the command then never performs would be misleading,
    so the notice comes after the output options are resolved - while still
    staying ahead of the Spark cold start.
    """
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter="\t",
        name="codes.tsv",
    )

    result = runner.invoke(
        cli, _member_of_args(dataset) + ["-o", str(tmp_path / "out.json")]
    )

    assert result.exit_code == 2
    assert "ndjson" in result.stderr.lower()
    assert "tab-separated" not in result.stderr


def test_from_offers_no_tsv_value(runner, patched_context, delimited_csv):
    """--from gains no tsv value; the extension governs the delimiter only."""
    dataset = delimited_csv(
        [["368529001", SNOMED]], header=["code", "system"], name="codes.tsv"
    )

    result = runner.invoke(cli, _member_of_args(dataset) + ["--from", "tsv"])

    assert result.exit_code == 2
    assert "is not one of" in result.stderr


# ========== Headerless CSV input (US3) ==========


def test_read_dataset_headerless_uses_positional_columns(pathling_ctx, delimited_csv):
    """_read_dataset treats the first line as data when input-header is off (T019)."""
    from pathling.cli.terminology import _read_dataset

    path = delimited_csv([["368529001", SNOMED]], header=None, name="headerless.csv")

    df = _read_dataset(pathling_ctx, str(path), "csv", input_header=False)

    # Spark assigns positional column names when there is no header row.
    assert df.columns == ["_c0", "_c1"]
    assert df.collect()[0]["_c0"] == "368529001"


def test_member_of_headerless_input(runner, patched_context, delimited_csv):
    """member-of runs against a headerless dataset via --no-input-header (T020)."""
    dataset = delimited_csv(
        [["368529001", SNOMED], ["439319006", SNOMED]],
        header=None,
        name="headerless.csv",
    )

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(dataset),
            "--no-input-header",
            "--code-column",
            "_c0",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    # The positional column names carry through to the output header.
    assert rows[0] == ["_c0", "_c1", "member_of"]
    assert rows[1][2] == "True"


# ========== Non-CSV no-op (T023) ==========


def test_output_options_ignored_for_ndjson(runner, patched_context, delimited_csv):
    """--delimiter/--header are accepted but do not affect NDJSON output (T023).

    The delimiter still applies to the CSV *input* read (here a semicolon file),
    but the NDJSON output path never consults the delimiter or header, so the
    result is unaffected JSON objects.
    """
    dataset = delimited_csv(
        [["368529001", SNOMED]],
        header=["code", "system"],
        delimiter=";",
        name="semi.csv",
    )

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "ndjson",
            "--delimiter",
            ";",
            "--no-header",
        ],
    )

    assert result.exit_code == 0, result.stderr
    line = result.stdout.splitlines()[0]
    record = json.loads(line)
    # The record keeps its keys and values; the header/delimiter had no effect.
    assert record["code"] == "368529001"
    assert "member_of" in record


def test_output_options_ignored_for_table(runner, patched_context, codes_csv):
    """--no-header does not suppress the table's header (T023)."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--no-header",
        ],
    )

    assert result.exit_code == 0, result.stderr
    # The table always carries its column names, regardless of --no-header.
    assert "member_of" in result.stdout


# ========== _detect_tabular_format ==========


def test_detect_csv_file(tmp_path):
    """A file ending in .csv is detected as CSV."""
    from pathling.cli.terminology import _detect_tabular_format

    path = _write_csv(tmp_path / "codes.csv", ["code"], [["a"]])
    assert _detect_tabular_format(path) == "csv"


def test_detect_csv_file_uppercase_suffix(tmp_path):
    """Suffix matching is case-insensitive, so .CSV is detected as CSV."""
    from pathling.cli.terminology import _detect_tabular_format

    path = _write_csv(tmp_path / "codes.CSV", ["code"], [["a"]])
    assert _detect_tabular_format(path) == "csv"


def test_detect_tsv_file(tmp_path):
    """A file ending in .tsv is detected as CSV, read with a tab delimiter."""
    from pathling.cli.terminology import _detect_tabular_format

    path = _write_csv(tmp_path / "codes.tsv", ["code"], [["a"]])
    assert _detect_tabular_format(path) == "csv"


def test_detect_parquet_file(tmp_path):
    """A file ending in .parquet is detected as Parquet."""
    from pathling.cli.terminology import _detect_tabular_format

    path = tmp_path / "codes.parquet"
    path.write_bytes(b"PAR1")
    assert _detect_tabular_format(path) == "parquet"


def test_detect_delta_directory(tmp_path):
    """A directory containing a _delta_log entry is detected as Delta."""
    from pathling.cli.terminology import _detect_tabular_format

    directory = tmp_path / "table"
    (directory / "_delta_log").mkdir(parents=True)
    (directory / "part-0.parquet").write_bytes(b"PAR1")
    assert _detect_tabular_format(directory) == "delta"


def test_detect_parquet_directory(tmp_path):
    """A directory with Parquet contents but no _delta_log is detected as Parquet."""
    from pathling.cli.terminology import _detect_tabular_format

    directory = tmp_path / "data"
    directory.mkdir()
    (directory / "part-00000.snappy.parquet").write_bytes(b"PAR1")
    (directory / "_SUCCESS").write_bytes(b"")
    assert _detect_tabular_format(directory) == "parquet"


def test_detect_delta_wins_over_parquet(tmp_path):
    """A directory with both _delta_log and .parquet files is detected as Delta."""
    from pathling.cli.terminology import _detect_tabular_format

    directory = tmp_path / "table"
    (directory / "_delta_log").mkdir(parents=True)
    (directory / "stray.parquet").write_bytes(b"PAR1")
    assert _detect_tabular_format(directory) == "delta"


def test_detect_unrecognised_file_suffix_raises(tmp_path):
    """An unrecognised file suffix is a usage error suggesting --from."""
    from pathling.cli.errors import EXIT_USAGE, CliError
    from pathling.cli.terminology import _detect_tabular_format

    path = tmp_path / "codes.txt"
    path.write_text("code\na\n")
    with raises(CliError) as info:
        _detect_tabular_format(path)
    assert info.value.exit_code == EXIT_USAGE
    assert "--from csv|parquet|delta" in str(info.value)


def test_detect_unrecognisable_directory_raises(tmp_path):
    """An unrecognisable directory is a usage error naming the path and contents."""
    from pathling.cli.errors import EXIT_USAGE, CliError
    from pathling.cli.terminology import _detect_tabular_format

    directory = tmp_path / "mystery"
    directory.mkdir()
    (directory / "notes.txt").write_text("hello")
    with raises(CliError) as info:
        _detect_tabular_format(directory)
    message = str(info.value)
    assert info.value.exit_code == EXIT_USAGE
    # The message names the offending path and suggests the flag.
    assert str(directory) in message
    assert "--from csv|parquet|delta" in message


# ========== member-of ==========


def test_member_of_with_system(runner, patched_context, codes_csv):
    """member-of appends a boolean membership column."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert rows[0] == ["code", "system", "member_of"]
    # 368529001 is a member of the refset; 439319006 is not.
    assert rows[1][2] == "True"


def test_member_of_with_system_column(runner, patched_context, codes_csv):
    """member-of can build codings from a per-row system column."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system-column",
            "system",
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[1][2] == "True"


def test_member_of_result_column_override(runner, patched_context, codes_csv):
    """--result-column renames the appended column."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--result-column",
            "is_member",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "is_member" in _stdout_rows(result)[0]


# ========== Error paths ==========


def test_system_and_system_column_mutually_exclusive(
    runner, patched_context, codes_csv
):
    """--system and --system-column together is a usage error."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--system-column",
            "system",
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.stderr.lower()


def test_no_system_is_usage_error(runner, patched_context, codes_csv):
    """Omitting both --system and --system-column is a usage error."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    assert "code system is required" in result.stderr.lower()


def test_missing_dataset_is_usage_error(runner, patched_context, tmp_path):
    """A missing dataset path fails before Spark with a usage error."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(tmp_path / "nope.csv"),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    assert "does not exist" in result.stderr.lower()


def test_missing_code_column_lists_columns(runner, patched_context, codes_csv):
    """A missing code column error lists the columns that do exist."""
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "nonexistent",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    assert "code" in result.stderr
    assert "system" in result.stderr


# ========== translate ==========


def test_translate(runner, patched_context, codes_csv):
    """translate appends translated system and code columns."""
    result = runner.invoke(
        cli,
        [
            "translate",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--concept-map",
            CONCEPT_MAP,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert "translated_code" in rows[0]
    # 368529001 translates to 368529002 under this concept map.
    assert any("368529002" in row for row in rows[1:])


# ========== subsumes / subsumed-by ==========


def test_subsumes(runner, patched_context, tmp_path):
    """subsumes appends a boolean column comparing two codings."""
    dataset = _write_csv(
        tmp_path / "pairs.csv", ["code", "other"], [["107963000", "63816008"]]
    )

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code-column",
            "other",
            "--other-system",
            SNOMED,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert "subsumes" in rows[0]
    assert rows[1][2] == "True"


def test_subsumed_by(runner, patched_context, tmp_path):
    """subsumed-by appends a boolean reverse-subsumption column."""
    dataset = _write_csv(
        tmp_path / "pairs.csv", ["code", "other"], [["63816008", "107963000"]]
    )

    result = runner.invoke(
        cli,
        [
            "subsumed-by",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code-column",
            "other",
            "--other-system",
            SNOMED,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[1][2] == "True"


def test_subsumes_with_fixed_other_code(runner, patched_context, tmp_path):
    """subsumes accepts a fixed target code applied to every row."""
    # A single code column, no target column in the data.
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["107963000"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "63816008",
            "--other-system",
            SNOMED,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert rows[0] == ["code", "subsumes"]
    # 107963000 subsumes the fixed target 63816008.
    assert rows[1][1] == "True"


def test_subsumed_by_with_fixed_other_code(runner, patched_context, tmp_path):
    """subsumed-by accepts a fixed target code applied to every row."""
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["63816008"]])

    result = runner.invoke(
        cli,
        [
            "subsumed-by",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "107963000",
            "--other-system",
            SNOMED,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert rows[0] == ["code", "subsumed_by"]
    # 63816008 is subsumed by the fixed target 107963000.
    assert rows[1][1] == "True"


def test_subsumes_fixed_other_code_result_column_override(
    runner, patched_context, tmp_path
):
    """--result-column renames the output column when a fixed target code is used."""
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["107963000"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "63816008",
            "--other-system",
            SNOMED,
            "--result-column",
            "is_ancestor",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    rows = _stdout_rows(result)
    assert rows[0] == ["code", "is_ancestor"]
    assert rows[1][1] == "True"


# ========== subsumes / subsumed-by target validation ==========


def test_other_code_and_other_code_column_mutually_exclusive(
    runner, patched_context, tmp_path
):
    """Supplying both --other-code and --other-code-column is a usage error."""
    dataset = _write_csv(tmp_path / "pairs.csv", ["code", "b"], [["107963000", "x"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "63816008",
            "--other-code-column",
            "b",
            "--other-system",
            SNOMED,
        ],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.stderr.lower()


def test_no_other_code_is_usage_error(runner, patched_context, tmp_path):
    """Omitting both --other-code and --other-code-column is a usage error."""
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["107963000"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-system",
            SNOMED,
        ],
    )

    assert result.exit_code == 2
    message = result.stderr.lower()
    assert "target code is required" in message
    # The message names both valid options.
    assert "--other-code" in result.stderr
    assert "--other-code-column" in result.stderr


def test_other_system_and_other_system_column_mutually_exclusive(
    runner, patched_context, tmp_path
):
    """Supplying both --other-system and --other-system-column is a usage error."""
    dataset = _write_csv(
        tmp_path / "codes.csv", ["code", "sys"], [["107963000", SNOMED]]
    )

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "63816008",
            "--other-system",
            SNOMED,
            "--other-system-column",
            "sys",
        ],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.stderr.lower()


def test_no_other_system_is_usage_error(runner, patched_context, tmp_path):
    """Omitting both --other-system and --other-system-column is a usage error."""
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["107963000"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-code",
            "63816008",
        ],
    )

    assert result.exit_code == 2
    message = result.stderr.lower()
    assert "target system is required" in message
    # The message names both valid options.
    assert "--other-system" in result.stderr
    assert "--other-system-column" in result.stderr


def test_target_validation_runs_before_context_creation(runner, monkeypatch, tmp_path):
    """Invalid target options fail before any Spark session is created.

    A spy replaces the context factory and fails if invoked, proving the
    target-side validation runs ahead of the Spark cold start (SC-002).
    """
    created = []

    def spy(config, console=None):
        created.append(True)
        raise AssertionError("context must not be created on a usage error")

    monkeypatch.setattr("pathling.cli.session.create_context", spy)
    dataset = _write_csv(tmp_path / "codes.csv", ["code"], [["107963000"]])

    result = runner.invoke(
        cli,
        [
            "subsumes",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--other-system",
            SNOMED,
        ],
    )

    assert result.exit_code == 2
    assert created == []


# ========== display / property-of / designation ==========


def test_display(runner, patched_context, tmp_path):
    """display appends the canonical display name."""
    dataset = _write_csv(tmp_path / "loinc.csv", ["code"], [["55915-3"]])

    result = runner.invoke(
        cli,
        [
            "display",
            str(dataset),
            "--code-column",
            "code",
            "--system",
            LOINC,
            "--accept-language",
            "en",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "Beta 2 globulin" in result.stdout


def test_property_of(runner, patched_context, codes_csv):
    """property-of appends the requested property values."""
    result = runner.invoke(
        cli,
        [
            "property-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--property",
            "parent",
            "--property-type",
            "code",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "property" in _stdout_rows(result)[0]
    # 439319006 has parent 785673007.
    assert "785673007" in result.stdout


def test_designation(runner, patched_context, codes_csv):
    """designation appends designation values, honouring --use."""
    result = runner.invoke(
        cli,
        [
            "designation",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--use",
            "http://terminology.hl7.org/CodeSystem/designation-usage|display",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "designation" in _stdout_rows(result)[0]


# ========== File output ==========


def test_output_to_csv_file(runner, patched_context, codes_csv, tmp_path):
    """Results can be written to a CSV file."""
    out = tmp_path / "out.csv"
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert out.exists()


def test_output_to_parquet_file(runner, patched_context, codes_csv, tmp_path):
    """Results can be written to a Parquet file."""
    out = tmp_path / "out.parquet"
    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert out.exists()


# ========== Unreachable server ==========


# A realistic Spark/HAPI connection failure message, including the square
# brackets that previously crashed the Rich-based error handler.
_BRACKETED_CONNECTION_ERROR = (
    "[FAILED_EXECUTE_UDF] User defined function failed due to: "
    "FhirClientConnectionException: Connect to 127.0.0.1:9 [/127.0.0.1] "
    "failed: Connection refused. SQLSTATE: 39000"
)


def test_unreachable_server_names_url(runner, patched_context, codes_csv, monkeypatch):
    """An unreachable server error names the URL and shows how to set it.

    The simulated error contains square brackets, reproducing the real JVM
    message that previously crashed the Rich error handler with a MarkupError.
    """

    def _boom(*args, **kwargs):
        raise RuntimeError(_BRACKETED_CONNECTION_ERROR)

    monkeypatch.setattr("pathling.udfs.member_of", _boom)

    result = runner.invoke(
        cli,
        [
            "--tx-server",
            "http://localhost:9999/fhir",
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 1
    assert "http://localhost:9999/fhir" in result.stderr
    assert "--tx-server" in result.stderr
    assert "tx-server" in result.stderr
    # The bracketed message must render without crashing the error handler.
    assert "MarkupError" not in result.stderr
    assert "Traceback" not in result.stderr
    assert "[FAILED_EXECUTE_UDF]" in result.stderr


def test_bracketed_runtime_error_renders_safely(
    runner, patched_context, codes_csv, monkeypatch
):
    """A non-connection error with square brackets renders without a traceback."""

    def _boom(*args, **kwargs):
        raise RuntimeError("[ANALYSIS_ERROR] something [unexpected] happened")

    monkeypatch.setattr("pathling.udfs.member_of", _boom)

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 1
    assert "MarkupError" not in result.stderr
    assert "Traceback" not in result.stderr
    assert "[ANALYSIS_ERROR]" in result.stderr


def test_local_mode_skips_server_enrichment(
    runner, patched_context, codes_csv, monkeypatch
):
    """In local mode a failure names the store path, not a terminology server.

    The connection-failure enrichment that names the remote server URL must be
    skipped when a store is configured (FR-011); the message instead names the
    store and suggests the import commands.
    """

    def _boom(*args, **kwargs):
        raise RuntimeError(_BRACKETED_CONNECTION_ERROR)

    monkeypatch.setattr("pathling.udfs.member_of", _boom)

    result = runner.invoke(
        cli,
        [
            "--tx-store",
            "/data/tx-store",
            "member-of",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 1
    # The store is named and the import commands suggested.
    assert "/data/tx-store" in result.stderr
    assert "import-snomed" in result.stderr
    # No terminology server URL is named, and the default is never mentioned.
    assert "terminology server" not in result.stderr.lower()
    assert "ontoserver" not in result.stderr


# ========== --from input format: explicit (US1) ==========


def test_from_delta_reads_delta_table(runner, patched_context, tmp_path):
    """display --from delta reads a Delta table directory (acceptance 1)."""
    table = _write_delta(
        patched_context.spark, tmp_path / "codes", ["code"], [["55915-3"]]
    )

    result = runner.invoke(
        cli,
        [
            "display",
            str(table),
            "--from",
            "delta",
            "--code-column",
            "code",
            "--system",
            LOINC,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "Beta 2 globulin" in result.stdout


def test_from_parquet_reads_parquet_directory(runner, patched_context, tmp_path):
    """member-of --from parquet reads a Parquet directory (acceptance 2)."""
    data = _write_parquet(
        patched_context.spark,
        tmp_path / "codes",
        ["code", "system"],
        [["368529001", SNOMED]],
    )

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(data),
            "--from",
            "parquet",
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[1][2] == "True"


def test_from_csv_reads_arbitrary_extension(runner, patched_context, tmp_path):
    """translate --from csv reads a CSV file with an arbitrary extension
    (acceptance 3)."""
    dataset = _write_csv(tmp_path / "codes.txt", ["code"], [["368529001"]])

    result = runner.invoke(
        cli,
        [
            "translate",
            str(dataset),
            "--from",
            "csv",
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--concept-map",
            CONCEPT_MAP,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert any("368529002" in row for row in _stdout_rows(result)[1:])


def test_from_invalid_choice_is_usage_error(runner, monkeypatch, codes_csv):
    """--from with an out-of-range value fails before Spark, listing the choices
    (acceptance 4)."""

    def spy(config, console=None):
        raise AssertionError("context must not be created on a usage error")

    monkeypatch.setattr("pathling.cli.session.create_context", spy)

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(codes_csv),
            "--from",
            "bogus",
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    # The Click choice error lists the three valid formats.
    assert "csv" in result.stderr
    assert "parquet" in result.stderr
    assert "delta" in result.stderr


def test_from_csv_missing_path_is_usage_error(runner, monkeypatch, tmp_path):
    """--from with a missing path fails before Spark with the existing error
    (acceptance 5)."""

    def spy(config, console=None):
        raise AssertionError("context must not be created on a usage error")

    monkeypatch.setattr("pathling.cli.session.create_context", spy)

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(tmp_path / "nope.csv"),
            "--from",
            "csv",
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
        ],
    )

    assert result.exit_code == 2
    assert "does not exist" in result.stderr.lower()


# ========== --from input format: auto-detection (US2) ==========


def test_autodetect_delta_directory(runner, patched_context, tmp_path):
    """A Delta directory is auto-detected without --from (acceptance 3)."""
    table = _write_delta(
        patched_context.spark, tmp_path / "codes", ["code"], [["55915-3"]]
    )

    result = runner.invoke(
        cli,
        [
            "display",
            str(table),
            "--code-column",
            "code",
            "--system",
            LOINC,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "Beta 2 globulin" in result.stdout


def test_autodetect_parquet_directory(runner, patched_context, tmp_path):
    """A Parquet directory is auto-detected without --from (acceptance 4)."""
    data = _write_parquet(
        patched_context.spark,
        tmp_path / "codes",
        ["code", "system"],
        [["368529001", SNOMED]],
    )

    result = runner.invoke(
        cli,
        [
            "member-of",
            str(data),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert _stdout_rows(result)[1][2] == "True"


def test_roundtrip_own_parquet_output(runner, patched_context, codes_csv, tmp_path):
    """The CLI's own Parquet output reads back into a second command without
    --from (SC-002)."""
    out = tmp_path / "out.parquet"
    first = runner.invoke(
        cli,
        [
            "display",
            str(codes_csv),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "-o",
            str(out),
        ],
    )
    assert first.exit_code == 0, first.stderr
    assert out.exists()

    # The second command reads the first command's Parquet output directory,
    # auto-detected as Parquet with no --from flag.
    second = runner.invoke(
        cli,
        [
            "member-of",
            str(out),
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert second.exit_code == 0, second.stderr
    assert "member_of" in _stdout_rows(second)[0]


def test_autodetect_undeterminable_directory_is_usage_error(
    runner, monkeypatch, tmp_path
):
    """An unrecognisable directory fails before Spark with an actionable error
    (acceptance 5)."""

    def spy(config, console=None):
        raise AssertionError("context must not be created on a usage error")

    monkeypatch.setattr("pathling.cli.session.create_context", spy)
    directory = tmp_path / "mystery"
    directory.mkdir()
    (directory / "notes.txt").write_text("nothing tabular here")

    result = runner.invoke(
        cli,
        [
            "display",
            str(directory),
            "--code-column",
            "code",
            "--system",
            SNOMED,
        ],
    )

    assert result.exit_code == 2
    # The message names the offending path and suggests the flag. Newlines are
    # stripped first because the console wraps the long message across lines,
    # which would otherwise split the path and the flag text mid-token.
    flattened = result.stderr.replace("\n", "")
    assert str(directory) in flattened
    assert "--from csv|parquet|delta" in flattened


# ========== Config precedence wiring ==========


def test_tx_server_flag_overrides_config(runner, monkeypatch, pathling_ctx, tmp_path):
    """The configured tx-server is honoured and overridden by the flag."""
    recorded = {}

    def spy(config, console=None):
        recorded["tx_server"] = config.tx_server
        return pathling_ctx

    monkeypatch.setattr("pathling.cli.session.create_context", spy)
    config_file = tmp_path / "config.toml"
    config_file.write_text('tx-server = "https://file.example/fhir"\n')
    codes = _write_csv(tmp_path / "codes.csv", ["code"], [["368529001"]])

    base = [
        "member-of",
        str(codes),
        "--code-column",
        "code",
        "--system",
        SNOMED,
        "--value-set",
        VALUE_SET,
        "--format",
        "csv",
    ]

    runner.invoke(cli, ["--config", str(config_file)] + base)
    assert recorded["tx_server"] == "https://file.example/fhir"

    runner.invoke(
        cli,
        ["--config", str(config_file), "--tx-server", "https://flag.example/fhir"]
        + base,
    )
    assert recorded["tx_server"] == "https://flag.example/fhir"
