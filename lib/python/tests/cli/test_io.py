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

"""Unit tests for data source format auto-detection.

Detection runs before Spark starts, so these tests build small fixture
directories and assert the detected format and the error paths.

Author: John Grimes.
"""

import json

import pytest

import pathling.cli.io as io_module
from pathling.cli.errors import CliError
from pathling.cli.io import (
    SourceFormat,
    SourceSpec,
    _looks_like_bundle,
    detect_format,
    discover_bundle_resource_types,
    read_single_resource,
    read_source,
    resolve_source,
)


def _bundle(resource_types):
    """Builds a minimal FHIR Bundle dict containing the given resource types."""
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {"resource": {"resourceType": rt, "id": "x"}} for rt in resource_types
        ],
    }


# ========== Directory detection ==========


def test_detects_ndjson(tmp_path):
    """A directory of .ndjson files is detected as ndjson."""
    (tmp_path / "Patient.ndjson").write_text('{"resourceType":"Patient"}\n')

    assert detect_format(tmp_path) == SourceFormat.NDJSON


def test_detects_ndjson_ignoring_other_files(tmp_path):
    """Stray non-FHIR files do not prevent ndjson detection."""
    (tmp_path / "Patient.ndjson").write_text('{"resourceType":"Patient"}\n')
    (tmp_path / "notes.txt").write_text("ignore me")

    assert detect_format(tmp_path) == SourceFormat.NDJSON


def test_detects_parquet(tmp_path):
    """A directory of *.parquet tables without a delta log is parquet."""
    table = tmp_path / "Patient.parquet"
    table.mkdir()
    (table / "part-00000.snappy.parquet").write_bytes(b"PAR1")

    assert detect_format(tmp_path) == SourceFormat.PARQUET


def test_detects_delta_by_delta_log(tmp_path):
    """A *.parquet table containing a _delta_log directory is delta."""
    table = tmp_path / "Patient.parquet"
    (table / "_delta_log").mkdir(parents=True)
    (table / "part-00000.snappy.parquet").write_bytes(b"PAR1")

    assert detect_format(tmp_path) == SourceFormat.DELTA


def test_detects_bundles(tmp_path):
    """A directory of JSON Bundle files is detected as bundles."""
    (tmp_path / "bundle1.json").write_text(json.dumps(_bundle(["Patient"])))

    assert detect_format(tmp_path) == SourceFormat.BUNDLES


# ========== Single resource detection ==========


def test_detects_single_resource_when_allowed(tmp_path):
    """A single JSON resource file is detected as a resource when allowed."""
    resource = tmp_path / "patient.json"
    resource.write_text('{"resourceType":"Patient","id":"x"}')

    assert detect_format(resource, allow_resource=True) == SourceFormat.RESOURCE


def test_single_file_rejected_when_not_allowed(tmp_path):
    """A single file is rejected as a data source when resource mode is off."""
    resource = tmp_path / "patient.json"
    resource.write_text('{"resourceType":"Patient"}')

    with pytest.raises(CliError):
        detect_format(resource, allow_resource=False)


# ========== Error paths ==========


def test_missing_path_errors_before_spark(tmp_path):
    """A missing path fails with a usage error naming the path."""
    missing = tmp_path / "does-not-exist"

    with pytest.raises(CliError) as exc_info:
        detect_format(missing)

    assert exc_info.value.exit_code == 2
    assert str(missing) in exc_info.value.message


def test_empty_directory_errors(tmp_path):
    """An empty directory fails with a usage error."""
    with pytest.raises(CliError) as exc_info:
        detect_format(tmp_path)

    assert exc_info.value.exit_code == 2
    assert "empty" in exc_info.value.message.lower()


def test_ambiguous_directory_lists_contents(tmp_path):
    """An undetectable directory lists what was found and shows --from."""
    (tmp_path / "mystery.dat").write_text("???")
    (tmp_path / "other.bin").write_text("???")

    with pytest.raises(CliError) as exc_info:
        detect_format(tmp_path)

    message = exc_info.value.message
    assert "mystery.dat" in message
    assert "--from" in message


# ========== resolve_source ==========


def test_resolve_source_with_explicit_from(tmp_path):
    """An explicit --from skips detection but still checks existence."""
    (tmp_path / "Patient.ndjson").write_text('{"resourceType":"Patient"}\n')

    spec = resolve_source(str(tmp_path), from_format=SourceFormat.PARQUET)

    assert spec.format == SourceFormat.PARQUET
    assert spec.path == tmp_path


def test_resolve_source_missing_with_from_errors(tmp_path):
    """An explicit --from with a missing path is still a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_source(str(tmp_path / "missing"), from_format=SourceFormat.NDJSON)

    assert exc_info.value.exit_code == 2


# ========== Bundle resource type discovery ==========


def test_discover_bundle_resource_types(tmp_path):
    """Distinct resource types across bundle entries are discovered."""
    (tmp_path / "a.json").write_text(json.dumps(_bundle(["Patient", "Condition"])))
    (tmp_path / "b.json").write_text(json.dumps(_bundle(["Patient", "Observation"])))

    assert discover_bundle_resource_types(tmp_path) == [
        "Condition",
        "Observation",
        "Patient",
    ]


def test_discover_bundle_resource_types_empty_errors(tmp_path):
    """A directory with no bundle resources is an error."""
    (tmp_path / "empty.json").write_text(json.dumps(_bundle([])))

    with pytest.raises(CliError):
        discover_bundle_resource_types(tmp_path)


# ========== Single resource reading ==========


def test_read_single_resource(tmp_path):
    """A single resource file yields its type and raw JSON."""
    resource = tmp_path / "patient.json"
    resource.write_text('{"resourceType":"Patient","id":"x"}')

    resource_type, text = read_single_resource(resource)

    assert resource_type == "Patient"
    assert "Patient" in text


def test_read_single_resource_without_type_errors(tmp_path):
    """A JSON file lacking resourceType is an error."""
    resource = tmp_path / "notaresource.json"
    resource.write_text('{"foo":"bar"}')

    with pytest.raises(CliError):
        read_single_resource(resource)


# ========== Additional edge cases ==========


def test_detects_xml_bundles(tmp_path):
    """A directory of XML Bundle files is detected as bundles."""
    (tmp_path / "bundle.xml").write_text('<?xml version="1.0"?><Bundle xmlns="x"/>')

    assert detect_format(tmp_path) == SourceFormat.BUNDLES


def test_ambiguous_directory_truncates_long_listing(tmp_path):
    """An ambiguous directory with many files truncates its listing."""
    for index in range(15):
        (tmp_path / f"file{index:02d}.dat").write_text("x")

    with pytest.raises(CliError) as exc_info:
        detect_format(tmp_path)

    assert "..." in exc_info.value.message


def test_read_source_rejects_resource_format():
    """read_source rejects the single-resource format as a data source."""
    with pytest.raises(CliError):
        read_source(None, SourceSpec(path=None, format=SourceFormat.RESOURCE))


# ========== Prefix-based bundle detection (US5) ==========


def test_looks_like_bundle_uses_leading_prefix(tmp_path):
    """Bundle detection decides from a leading prefix, not a full parse.

    The file is a valid Bundle in its prefix but invalid JSON after it; a full
    ``json.load`` would fail, so a True result proves only the prefix was read
    (FR-014).
    """
    path = tmp_path / "big.json"
    path.write_text(
        '{"resourceType": "Bundle", "type": "collection", "entry": [ this is '
        "not valid json after the prefix",
        encoding="utf-8",
    )

    assert _looks_like_bundle(path) is True


def test_looks_like_bundle_false_for_non_bundle(tmp_path):
    """A non-Bundle resource file is not detected as a Bundle."""
    path = tmp_path / "patient.json"
    path.write_text('{"resourceType": "Patient", "id": "x"}', encoding="utf-8")

    assert _looks_like_bundle(path) is False


# ========== read_source resource-type passthrough (US5) ==========


class _RecordingReader:
    """Records the arguments passed to ``bundles``."""

    def __init__(self):
        self.bundles_args = None

    def bundles(self, path, types):
        self.bundles_args = (path, types)
        return "data-source"


class _RecordingPc:
    """A minimal context exposing a recording ``read`` reader."""

    def __init__(self):
        self.read = _RecordingReader()


def test_read_source_with_types_skips_discovery(tmp_path, monkeypatch):
    """An explicit ``types`` argument is used directly and skips discovery
    (FR-015)."""
    calls = []
    monkeypatch.setattr(
        io_module,
        "discover_bundle_resource_types",
        lambda path: calls.append(path) or ["should-not-be-used"],
    )
    pc = _RecordingPc()
    spec = SourceSpec(path=tmp_path, format=SourceFormat.BUNDLES)

    result = read_source(pc, spec, types=["Patient", "Condition"])

    assert result == "data-source"
    assert calls == []
    assert pc.read.bundles_args == (str(tmp_path), ["Patient", "Condition"])


def test_read_source_types_inert_for_non_bundles(tmp_path, monkeypatch):
    """An explicit ``types`` argument has no effect on a non-Bundles source.

    Covers the spec edge case that ``convert --type`` must not change behaviour
    for ndjson/parquet/delta inputs.
    """
    calls = []
    monkeypatch.setattr(
        io_module,
        "discover_bundle_resource_types",
        lambda path: calls.append(path) or ["should-not-be-used"],
    )

    class _NdjsonReader:
        def __init__(self):
            self.ndjson_path = None

        def ndjson(self, path):
            self.ndjson_path = path
            return "ndjson-data-source"

    class _Pc:
        read = _NdjsonReader()

    pc = _Pc()
    spec = SourceSpec(path=tmp_path, format=SourceFormat.NDJSON)

    result = read_source(pc, spec, types=["Patient", "Condition"])

    assert result == "ndjson-data-source"
    assert pc.read.ndjson_path == str(tmp_path)
    # Discovery is never consulted for a non-Bundles source.
    assert calls == []


def test_read_source_without_types_runs_discovery(tmp_path, monkeypatch):
    """With no explicit types, discovery runs to enumerate the bundle types."""
    calls = []
    monkeypatch.setattr(
        io_module,
        "discover_bundle_resource_types",
        lambda path: calls.append(path) or ["Patient"],
    )
    pc = _RecordingPc()
    spec = SourceSpec(path=tmp_path, format=SourceFormat.BUNDLES)

    read_source(pc, spec)

    assert calls == [tmp_path]
    assert pc.read.bundles_args == (str(tmp_path), ["Patient"])
