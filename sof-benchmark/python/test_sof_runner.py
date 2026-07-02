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

"""Unit tests for the pure logic of the Python SQL-on-FHIR benchmark runner.

These mirror the Java tests (deterministic data location, checkfile assertions keyed by
case id, countVariancePermitted, sha256 verification, and the contract-v2 report shape) and
run without a Spark session — only the runner's pure helpers are exercised.
"""

import json
from pathlib import Path

import pytest

import sof_runner

FIXTURES = Path(__file__).resolve().parent.parent / "src/test/resources/contract-v2"
BENCHMARK_FILE = FIXTURES / "clinical-flat.json"
DATA_ROOT = FIXTURES / "data"
CONDITION_SHA = "64c58f6cde2353e62b6eefc26a27291ff4e7369e11962564891784bb1f2faf11"
OBSERVATION_SHA = "3d15d1ac66e72bbf139e4b34a805c7582ca00c7e4d3e886a8fb7e56bdcc55518"


def _checkfile():
    return sof_runner.load_checkfile(BENCHMARK_FILE)


def test_locate_data_dir_resolves_identity_path():
    data_dir = sof_runner.locate_data_dir(DATA_ROOT, "synthea-clinical", "1", "s")
    assert data_dir.is_dir()
    assert (data_dir / "Condition.ndjson").is_file()
    assert data_dir.match("synthea-clinical/1/s")


def test_locate_data_dir_missing_reports_identity():
    with pytest.raises(FileNotFoundError) as error:
        sof_runner.locate_data_dir(DATA_ROOT, "synthea-clinical", "1", "xl")
    message = str(error.value)
    assert "synthea-clinical" in message
    assert "version=1" in message
    assert "size=xl" in message


def test_checkfile_sibling_swaps_extension():
    sibling = sof_runner.checkfile_sibling(Path("/tmp/suite/clinical-flat.json"))
    assert sibling.name == "clinical-flat.check.json"


def test_checkfile_assertions_counts_and_checksums():
    checkfile = _checkfile()
    assert sof_runner.expected_count(checkfile, "condition-flat", "s") == 2
    assert sof_runner.expected_count(checkfile, "observation-components", "s") == 2
    assert sof_runner.expected_count(checkfile, "active-conditions", "s") == 1
    assert sof_runner.expected_count(checkfile, "condition-flat", "unknown") is None
    assert sof_runner.expected_count(checkfile, "unknown-case", "s") is None

    assert sof_runner.resource_counts(checkfile, "s") == {
        "Condition": 2,
        "Observation": 2,
    }
    assert (
        sof_runner.file_checksums(checkfile, "s")["Condition.ndjson"] == CONDITION_SHA
    )


def test_absent_checkfile_is_none_and_helpers_are_empty():
    schema_only = FIXTURES / "benchmark-report.schema.json"
    checkfile = sof_runner.load_checkfile(schema_only)
    assert checkfile is None
    assert sof_runner.expected_count(checkfile, "condition-flat", "s") is None
    assert sof_runner.resource_counts(checkfile, "s") == {}
    assert sof_runner.file_checksums(checkfile, "s") == {}


def test_verify_checksums_matches_reports_and_missing():
    data_dir = DATA_ROOT / "synthea-clinical" / "1" / "s"
    assert (
        sof_runner.verify_checksums(
            data_dir,
            {"Condition.ndjson": CONDITION_SHA, "Observation.ndjson": OBSERVATION_SHA},
        )
        == []
    )

    drift = sof_runner.verify_checksums(data_dir, {"Condition.ndjson": "0" * 64})
    assert len(drift) == 1 and "drift" in drift[0]

    missing = sof_runner.verify_checksums(data_dir, {"Encounter.ndjson": CONDITION_SHA})
    assert len(missing) == 1 and "missing" in missing[0]


def test_correctness_status():
    assert sof_runner.correctness_status(2, 2, False) == "ok"
    assert sof_runner.correctness_status(2, 3, False) == "count_mismatch"
    assert sof_runner.correctness_status(None, 3, False) == "ok"
    assert sof_runner.correctness_status(2, 3, True) == "ok"


def test_stats_for_carries_exactly_the_fixed_key_set():
    stats = sof_runner.stats_for([10.0, 12.0, 11.0])
    assert set(stats.keys()) == {"mean", "stddev", "min", "max", "median"}
    assert sof_runner.stats_for([]) == {}


def _sample_report():
    benchmark = json.loads(BENCHMARK_FILE.read_text())
    results = [
        {
            "id": "condition-flat",
            "status": "ok",
            "inputRows": 2,
            "outputRows": 2,
            "samplesMs": [10.0, 12.0, 11.0],
            "stats": sof_runner.stats_for([10.0, 12.0, 11.0]),
            "phaseSamplesMs": {"load": [42.0], "executeExtract": [10.0, 12.0, 11.0]},
        }
    ]
    return sof_runner.build_report(
        "9.9.0",
        "9.9.0",
        benchmark,
        benchmark["dataset"],
        {"os": "Test OS"},
        "csv",
        1,
        3,
        "s",
        "4.0.1",
        sof_runner.resource_counts(_checkfile(), "s"),
        results,
    )


def test_build_report_v2_shape():
    report = _sample_report()

    assert report["implementation"]["engine"]["name"] == "Pathling"
    assert report["implementation"]["binding"]["name"] == "pathling-python"
    assert report["benchmark"] == {"name": "clinical-flat", "version": "1"}
    assert report["dataset"] == {"name": "synthea-clinical", "version": "1"}
    assert "benchmarkVersion" not in report
    assert "name" not in report["implementation"]

    measurement = report["measurement"]
    assert measurement["scenario"] == "preloaded_repeated"
    assert measurement["sink"] == "csv"

    entry = report["results"]["clinical-flat"]
    assert entry["resourceCounts"]["Condition"] == 2
    assert entry["cases"][0]["id"] == "condition-flat"
    assert "title" not in entry["cases"][0]


def test_parity_with_java_report_shape():
    """The Python report shares the Java report's shape, differing only by the binding name."""
    report = _sample_report()

    # Same top-level keys as the Java ReportWriter emits.
    assert set(report.keys()) == {
        "implementation",
        "benchmark",
        "dataset",
        "environment",
        "measurement",
        "results",
    }
    # The engine identity is shared across bindings; only the binding name differs.
    assert report["implementation"]["engine"]["name"] == "Pathling"
    assert report["implementation"]["binding"]["name"] == "pathling-python"
    assert set(report["measurement"].keys()) == {
        "scenario",
        "phases",
        "sink",
        "warmup",
        "iterations",
    }
    case = report["results"]["clinical-flat"]["cases"][0]
    assert set(case.keys()) == {
        "id",
        "status",
        "inputRows",
        "outputRows",
        "samplesMs",
        "stats",
        "phaseSamplesMs",
    }
    assert set(case["stats"].keys()) == {"mean", "stddev", "min", "max", "median"}


def test_run_isolated_records_a_failing_case_as_execution_error():
    def boom():
        raise RuntimeError("boom: could not evaluate view")

    result = sof_runner.run_isolated("broken-case", boom)

    assert result == {
        "id": "broken-case",
        "status": "execution_error",
        "message": "boom: could not evaluate view",
    }


def test_run_isolated_returns_a_succeeding_case_unchanged():
    measured = {"id": "ok-case", "status": "ok", "inputRows": 2, "outputRows": 2}

    result = sof_runner.run_isolated("ok-case", lambda: measured)

    assert result is measured
    assert "message" not in result


def test_one_failing_case_does_not_abort_the_run_nor_void_the_others():
    def measurement_for(case_id):
        if case_id == "second":
            raise ValueError  # No message: falls back to repr().
        return {"id": case_id, "status": "ok"}

    ids = ["first", "second", "third"]
    results = [sof_runner.run_isolated(i, lambda i=i: measurement_for(i)) for i in ids]

    assert [r["id"] for r in results] == ["first", "second", "third"]
    assert results[0]["status"] == "ok"
    assert results[1]["status"] == "execution_error"
    assert results[1]["message"]
    assert results[2]["status"] == "ok"
