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

"""Integration tests for the ``pathling convert`` command.

These tests drive the command through Click's ``CliRunner`` against the shared
Spark fixture, exercising format conversions, save modes, and error paths.

Author: John Grimes.
"""

import os
import shutil

from pytest import fixture

from pathling.cli.main import cli

PATIENT_COUNT = 9


@fixture(scope="module")
def ndjson_input(test_data_dir, tmp_path_factory):
    """A clean ndjson input directory with just Patient and Condition."""
    source = os.path.join(test_data_dir, "ndjson")
    target = tmp_path_factory.mktemp("convert-input")
    for name in ("Patient.ndjson", "Condition.ndjson"):
        shutil.copy(os.path.join(source, name), target / name)
    return target


def _count_ndjson_lines(directory, resource_type):
    """Counts the JSON lines written for a resource type in a directory."""
    total = 0
    for entry in directory.iterdir():
        if entry.name.startswith(resource_type) and entry.suffix == ".ndjson":
            with open(entry) as handle:
                total += sum(1 for line in handle if line.strip())
    return total


# ========== ndjson to parquet ==========


def test_ndjson_to_parquet_with_summary(
    runner, patched_context, ndjson_input, tmp_path
):
    """Converting ndjson to Parquet writes tables and prints a summary."""
    out = tmp_path / "warehouse"

    result = runner.invoke(
        cli, ["convert", str(ndjson_input), "--to", "parquet", "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    # A Patient parquet table is written under the output directory.
    written = sorted(entry.name for entry in out.iterdir())
    assert any(name.startswith("Patient") for name in written), written
    # The summary names the resource types and counts (on stderr).
    assert "Patient" in result.stderr
    assert str(PATIENT_COUNT) in result.stderr


# ========== Round trip ==========


def test_parquet_to_ndjson_round_trip(runner, patched_context, ndjson_input, tmp_path):
    """A parquet round trip preserves the Patient resource count."""
    warehouse = tmp_path / "warehouse"
    roundtrip = tmp_path / "roundtrip"

    first = runner.invoke(
        cli, ["convert", str(ndjson_input), "--to", "parquet", "-o", str(warehouse)]
    )
    assert first.exit_code == 0, first.stderr

    second = runner.invoke(
        cli, ["convert", str(warehouse), "--to", "ndjson", "-o", str(roundtrip)]
    )
    assert second.exit_code == 0, second.stderr
    assert _count_ndjson_lines(roundtrip, "Patient") == PATIENT_COUNT


# ========== Bundles input ==========


def test_bundles_input(runner, patched_context, test_data_dir, tmp_path):
    """Bundles input is auto-detected and converted to ndjson."""
    bundles = os.path.join(test_data_dir, "bundles")
    out = tmp_path / "from-bundles"

    result = runner.invoke(
        cli, ["convert", bundles, "--from", "bundles", "--to", "ndjson", "-o", str(out)]
    )

    assert result.exit_code == 0, result.stderr
    assert _count_ndjson_lines(out, "Patient") > 0


# ========== Delta output and merge ==========


def test_delta_output_and_merge(runner, patched_context, ndjson_input, tmp_path):
    """Delta output is written and can be re-written with --mode merge."""
    out = tmp_path / "delta-warehouse"

    first = runner.invoke(
        cli, ["convert", str(ndjson_input), "--to", "delta", "-o", str(out)]
    )
    assert first.exit_code == 0, first.stderr
    assert (out / "Patient.parquet" / "_delta_log").exists()

    merged = runner.invoke(
        cli,
        [
            "convert",
            str(ndjson_input),
            "--to",
            "delta",
            "-o",
            str(out),
            "--mode",
            "merge",
        ],
    )
    assert merged.exit_code == 0, merged.stderr


# ========== Error paths ==========


def test_existing_output_without_overwrite_fails(
    runner, patched_context, ndjson_input, tmp_path
):
    """Writing to an existing path without --overwrite fails showing the flag."""
    out = tmp_path / "warehouse"
    out.mkdir()

    result = runner.invoke(
        cli, ["convert", str(ndjson_input), "--to", "parquet", "-o", str(out)]
    )

    assert result.exit_code == 1
    assert "--overwrite" in result.stderr


def test_existing_output_with_overwrite_succeeds(
    runner, patched_context, ndjson_input, tmp_path
):
    """Passing --overwrite allows replacing an existing output path."""
    out = tmp_path / "warehouse"

    first = runner.invoke(
        cli, ["convert", str(ndjson_input), "--to", "parquet", "-o", str(out)]
    )
    assert first.exit_code == 0, first.stderr

    again = runner.invoke(
        cli,
        [
            "convert",
            str(ndjson_input),
            "--to",
            "parquet",
            "-o",
            str(out),
            "--overwrite",
        ],
    )
    assert again.exit_code == 0, again.stderr


def test_merge_mode_validation_for_non_delta(
    runner, patched_context, ndjson_input, tmp_path
):
    """The merge save mode is rejected for non-delta output as a usage error."""
    out = tmp_path / "warehouse"

    result = runner.invoke(
        cli,
        [
            "convert",
            str(ndjson_input),
            "--to",
            "parquet",
            "-o",
            str(out),
            "--mode",
            "merge",
        ],
    )

    assert result.exit_code == 2
    assert "merge" in result.stderr.lower()


def test_ambiguous_format_error(runner, patched_context, tmp_path):
    """An undetectable input directory fails with guidance to use --from."""
    ambiguous = tmp_path / "mixed"
    ambiguous.mkdir()
    (ambiguous / "mystery.dat").write_text("???")

    result = runner.invoke(
        cli, ["convert", str(ambiguous), "--to", "ndjson", "-o", str(tmp_path / "out")]
    )

    assert result.exit_code == 2
    assert "--from" in result.stderr
