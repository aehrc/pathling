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

import io as _io
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

from pytest import fixture
from rich.console import Console

from pathling.cli.main import cli

PATIENT_COUNT = 9


class _FakeSink:
    """A no-op write sink capturing nothing; the write path is exercised
    elsewhere."""

    def ndjson(self, target, save_mode=None):
        pass

    def parquet(self, target, save_mode=None):
        pass

    def delta(self, target, save_mode=None):
        pass


class _FakeDataSource:
    """A stand-in data source for convert without Spark.

    ``read`` raises so any per-type ``count()`` in the summary is caught.
    """

    def __init__(self, resource_types):
        self._resource_types = resource_types
        self.write = _FakeSink()

    def resource_types(self):
        return self._resource_types

    def read(self, resource_type):
        raise AssertionError("the summary must not read per-type counts")


class _FakeReader:
    """Records the resource types passed to ``bundles``."""

    def __init__(self, resource_types):
        self._resource_types = resource_types
        self.bundles_types = None

    def bundles(self, path, types):
        self.bundles_types = types
        return _FakeDataSource(self._resource_types)


class _FakePc:
    """A minimal context exposing a recording ``read`` reader."""

    def __init__(self, resource_types):
        self.read = _FakeReader(resource_types)


def _bundles_dir(tmp_path):
    """Creates a minimal directory of FHIR Bundle JSON for convert tests."""
    bundles = tmp_path / "bundles"
    bundles.mkdir()
    (bundles / "b.json").write_text(
        '{"resourceType":"Bundle","type":"collection","entry":[]}', encoding="utf-8"
    )
    return bundles


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
    # The summary names the resource types written, but no longer triggers a
    # per-type row count (FR-013). The output path is also reported, but is not
    # asserted here as Rich wraps the long temp path across lines in the narrow
    # test console; the path is covered by test_summary_does_not_count_per_type.
    assert "Patient" in result.stderr
    assert "Condition" in result.stderr


# ========== Summary, --type, and discovery (US5, no Spark) ==========


def test_summary_does_not_count_per_type():
    """The conversion summary lists resource types without a per-type count.

    The fake data source raises from ``read`` so any ``read().count()`` in the
    summary would fail the test (FR-013).
    """
    from pathling.cli.convert import _print_summary

    buffer = _io.StringIO()
    console = Console(file=buffer, width=200, highlight=False)

    _print_summary(console, _FakeDataSource(["Condition", "Patient"]), Path("/out"))

    rendered = buffer.getvalue()
    assert "Patient" in rendered
    assert "Condition" in rendered
    assert "/out" in rendered


def test_convert_type_passes_through_and_skips_discovery(runner, monkeypatch, tmp_path):
    """`--type` is used directly and skips driver-side discovery (FR-015)."""
    import pathling.cli.io as io_module

    discovery_calls = []
    monkeypatch.setattr(
        io_module,
        "discover_bundle_resource_types",
        lambda path: discovery_calls.append(path) or ["unused"],
    )
    pc = _FakePc(["Patient", "Condition"])
    monkeypatch.setattr(
        "pathling.cli.session.create_context", lambda config, console=None: pc
    )

    bundles = _bundles_dir(tmp_path)
    out = tmp_path / "out"

    result = runner.invoke(
        cli,
        [
            "convert",
            str(bundles),
            "--from",
            "bundles",
            "--to",
            "ndjson",
            "-o",
            str(out),
            "--type",
            "Patient",
            "--type",
            "Condition",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert discovery_calls == []
    assert pc.read.bundles_types == ["Patient", "Condition"]


def test_convert_discovery_runs_under_spinner(runner, monkeypatch, tmp_path):
    """Without `--type`, bundle discovery runs under the progress spinner rather
    than silently before it (FR-016)."""
    import pathling.cli.convert as convert_module
    import pathling.cli.io as io_module

    active = []
    snapshot = {}

    @contextmanager
    def fake_progress(console, message, verbose):
        active.append(message)
        try:
            yield
        finally:
            active.pop()

    monkeypatch.setattr(convert_module, "progress_status", fake_progress)

    def fake_discover(path):
        # Record the active spinner messages at the moment discovery runs.
        snapshot["active"] = list(active)
        return ["Patient"]

    monkeypatch.setattr(io_module, "discover_bundle_resource_types", fake_discover)

    pc = _FakePc(["Patient"])
    monkeypatch.setattr(
        "pathling.cli.session.create_context", lambda config, console=None: pc
    )

    bundles = _bundles_dir(tmp_path)
    out = tmp_path / "out"

    result = runner.invoke(
        cli,
        [
            "convert",
            str(bundles),
            "--from",
            "bundles",
            "--to",
            "ndjson",
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.stderr
    # Discovery ran while at least one progress spinner was active.
    assert snapshot["active"]


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
