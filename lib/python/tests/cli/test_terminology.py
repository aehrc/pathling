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

from pytest import fixture

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
