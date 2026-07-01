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

"""SQL-on-FHIR benchmark runner over the Pathling Python API (contract v2).

This mirrors the Java ``SofBenchmarkRunner``: it resolves a benchmark's materialized
NDJSON deterministically at ``<dataRoot>/<name>/<version>/<size>/``, verifies the loaded
files against the sibling checkfile's sha256 lock, separates the FHIR load/encode phase
from the timed execute+extract region, times a full CSV write of each case's result
(never a lazy count), checks the output row count against the checkfile assertion keyed by
the case ``id`` (honouring ``countVariancePermitted``), and emits a report conforming to
``benchmark-report.schema.json`` with ``implementation.engine.name = "Pathling"`` and
``implementation.binding.name = "pathling-python"``.
"""

import hashlib
import json
import logging
import os
import platform
import statistics
import tempfile
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Dict, List, Optional

ENGINE_NAME = "Pathling"
BINDING_NAME = "pathling-python"
SCENARIO = "preloaded_repeated"
DEFAULT_SINK = "csv"
DEFAULT_OUT = "benchmark-report.json"
DEFAULT_WARMUP = 1
DEFAULT_MEASUREMENT = 5

# The sink categories permitted by ``benchmark-report.schema.json``.
SCHEMA_SINKS = frozenset({"table", "csv", "memory", "other"})

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("sof_runner")


def locate_data_dir(data_root: Path, name: str, version_id: str, size: str) -> Path:
    """Resolves the size-specific data directory deterministically from dataset identity.

    Under contract v2 the materializer writes data to ``<data_root>/<name>/<version>/<size>/``,
    so the directory is resolved directly from the explicit ``(name, version)`` identity rather
    than scanning manifests or reproducing a recipe hash.

    :param data_root: the root directory containing materialized datasets.
    :param name: the dataset name (first directory segment).
    :param version_id: the dataset version (second directory segment).
    :param size: the size key (third directory segment).
    :return: the directory containing the size's NDJSON files.
    :raises FileNotFoundError: if the resolved directory does not exist.
    """
    data_dir = data_root / name / version_id / size
    if not data_dir.is_dir():
        raise FileNotFoundError(
            f"No materialized data at {data_dir} for (name={name}, version={version_id}, "
            f"size={size}). Run the materializer first: "
            f"cd sql-on-fhir/benchmark && bun run data <file> --size {size}"
        )
    return data_dir


def checkfile_sibling(benchmark_file: Path) -> Path:
    """Resolves the checkfile beside a benchmark file by swapping ``.json`` for ``.check.json``.

    :param benchmark_file: the path to the benchmark ``*.json`` file.
    :return: the sibling ``*.check.json`` path.
    """
    base = benchmark_file.name.removesuffix(".json")
    return benchmark_file.resolve().with_name(base + ".check.json")


def load_checkfile(benchmark_file: Path) -> Optional[dict]:
    """Loads the checkfile beside a benchmark file, or None when none exists.

    :param benchmark_file: the path to the benchmark ``*.json`` file.
    :return: the parsed checkfile dict, or None when the sibling is absent.
    """
    checkfile = checkfile_sibling(benchmark_file)
    if not checkfile.is_file():
        return None
    return json.loads(checkfile.read_text())


def expected_count(checkfile: Optional[dict], case_id: str, size: str) -> Optional[int]:
    """Returns the checkfile assertion for a case and size, if present.

    :param checkfile: the parsed checkfile, or None.
    :param case_id: the stable case id.
    :param size: the size key.
    :return: the expected output row count, or None when no assertion is declared.
    """
    if checkfile is None:
        return None
    return checkfile.get("assertions", {}).get(case_id, {}).get(size)


def resource_counts(checkfile: Optional[dict], size: str) -> Dict[str, int]:
    """Returns the per-resource-type row counts locked for a size.

    :param checkfile: the parsed checkfile, or None.
    :param size: the size key.
    :return: the resource counts, or an empty dict when unavailable.
    """
    if checkfile is None:
        return {}
    return checkfile.get("sizes", {}).get(size, {}).get("resourceCounts", {})


def file_checksums(checkfile: Optional[dict], size: str) -> Dict[str, str]:
    """Returns the per-file sha256 map locked for a size.

    :param checkfile: the parsed checkfile, or None.
    :param size: the size key.
    :return: a map of file name to sha256, or an empty dict when unavailable.
    """
    if checkfile is None:
        return {}
    files = checkfile.get("sizes", {}).get(size, {}).get("files", {})
    return {name: entry.get("sha256") for name, entry in files.items()}


def sha256_of(path: Path) -> str:
    """Computes the sha256 of a file as a lower-case hex string.

    :param path: the file to hash.
    :return: the lower-case hex sha256.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_checksums(data_dir: Path, locked: Dict[str, str]) -> List[str]:
    """Verifies each locked file's sha256 against the corresponding NDJSON in the data directory.

    :param data_dir: the directory containing the materialized ``<ResourceType>.ndjson`` files.
    :param locked: the checkfile's file name to sha256 map for the size.
    :return: the list of human-readable drift messages; empty when every locked file matches.
    """
    drift: List[str] = []
    for file_name, locked_sha in locked.items():
        file_path = data_dir / file_name
        if not file_path.is_file():
            drift.append(f"{file_name}: missing (locked {locked_sha})")
            continue
        actual = sha256_of(file_path)
        if actual != locked_sha:
            drift.append(
                f"{file_name}: sha256 drift (locked {locked_sha}, actual {actual})"
            )
    return drift


def correctness_status(
    expected: Optional[int], output_rows: int, variance_permitted: bool
) -> str:
    """Determines a case's correctness status.

    Present and equal is ``ok``; present and unequal is ``count_mismatch``; absent is ``ok``.
    When count variance is permitted a divergence is never flagged.

    :param expected: the checkfile assertion for this case and size, if any.
    :param output_rows: the observed output row count.
    :param variance_permitted: whether a count divergence must not be flagged.
    :return: ``ok`` or ``count_mismatch``.
    """
    if variance_permitted:
        return "ok"
    if expected is None:
        return "ok"
    return "ok" if expected == output_rows else "count_mismatch"


def schema_sink(sink: str) -> str:
    """Maps a Spark write format to a sink category permitted by the report schema.

    :param sink: the Spark write format used for the timed region.
    :return: a schema-valid sink category (``other`` for formats not enumerated by the schema).
    """
    return sink if sink in SCHEMA_SINKS else "other"


def stats_for(samples: List[float]) -> Dict[str, float]:
    """Computes the fixed contract-v2 statistics set over timing samples.

    :param samples: the timing samples in milliseconds.
    :return: a dict with exactly ``mean``, ``stddev``, ``min``, ``max`` and ``median`` (empty
        when there are no samples).
    """
    if not samples:
        return {}
    return {
        "mean": statistics.fmean(samples),
        "stddev": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "min": min(samples),
        "max": max(samples),
        "median": statistics.median(samples),
    }


def build_report(
    engine_version: str,
    binding_version: str,
    benchmark: dict,
    dataset: dict,
    env: Dict[str, object],
    sink: str,
    warmup: int,
    iterations: int,
    size: str,
    fhir_version: str,
    counts: Dict[str, int],
    results: List[dict],
) -> dict:
    """Builds a contract-v2 benchmark report dict.

    :param engine_version: the execution engine version.
    :param binding_version: the language binding version.
    :param benchmark: the parsed benchmark file (for suite ``name``/``version``).
    :param dataset: the benchmark's ``dataset`` block (for ``name``/``version``).
    :param env: the environment description.
    :param sink: the schema-valid sink category.
    :param warmup: the actual warmup iteration count.
    :param iterations: the actual measured iteration count.
    :param size: the size key.
    :param fhir_version: the FHIR version.
    :param counts: the per-resource-type row counts observed at this size.
    :param results: the per-case result dicts.
    :return: the report dict, keyed under ``results`` by the authored suite name.
    """
    return {
        "implementation": {
            "engine": {"name": ENGINE_NAME, "version": engine_version},
            "binding": {"name": BINDING_NAME, "version": binding_version},
        },
        "benchmark": {"name": benchmark["name"], "version": benchmark["version"]},
        "dataset": {"name": dataset["name"], "version": dataset["version"]},
        "environment": env,
        "measurement": {
            "scenario": SCENARIO,
            "phases": ["execute", "extract"],
            "sink": sink,
            "warmup": warmup,
            "iterations": iterations,
        },
        "results": {
            benchmark["name"]: {
                "size": size,
                "fhirVersion": fhir_version,
                "resourceCounts": counts,
                "cases": results,
            }
        },
    }


def resolve_version() -> str:
    """Resolves the installed Pathling package version.

    :return: the Pathling version, or "UNKNOWN" if it cannot be determined.
    """
    try:
        return version("pathling")
    except PackageNotFoundError:
        return "UNKNOWN"


def environment(pc) -> Dict[str, object]:
    """Builds the report's environment block.

    :param pc: the Pathling context.
    :return: a dict describing the OS, Java, Spark and core count.
    """
    java_version = pc.spark._jvm.System.getProperty("java.version")
    return {
        "os": platform.platform(),
        "java": java_version,
        "spark": pc.spark.version,
        "cores": os.cpu_count(),
    }


def _verify_dataset_integrity(
    data_dir: Path, checkfile: Optional[dict], size: str
) -> None:
    """Logs the outcome of verifying loaded NDJSON against the checkfile sha256 lock."""
    if checkfile is None:
        log.warning(
            "No checkfile beside the benchmark file; dataset integrity is unverified and "
            "output row counts are not checked."
        )
        return
    drift = verify_checksums(data_dir, file_checksums(checkfile, size))
    if not drift:
        log.info("Dataset integrity verified against the checkfile for size %s.", size)
    else:
        for message in drift:
            log.error("Dataset integrity: %s", message)
        log.error(
            "Loaded data does not match the checkfile lock; timings are NOT verified "
            "against the locked dataset."
        )


def main() -> None:
    """Run the SQL-on-FHIR benchmark with the Pathling Python API."""
    import click
    from pathling import PathlingContext

    @click.command()
    @click.argument(
        "benchmark_file",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
    )
    @click.option("--size", required=True, help="Size key to run (e.g. s or m).")
    @click.option(
        "--data",
        "data_root",
        required=True,
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        help="Root directory of materialized datasets.",
    )
    @click.option(
        "--sink",
        type=click.Choice(["csv", "parquet"]),
        default=DEFAULT_SINK,
        show_default=True,
        help="Spark write format for the timed region.",
    )
    @click.option(
        "--out",
        type=click.Path(dir_okay=False, path_type=Path),
        default=Path(DEFAULT_OUT),
        show_default=True,
        help="Report output path.",
    )
    def run(
        benchmark_file: Path, size: str, data_root: Path, sink: str, out: Path
    ) -> None:
        benchmark = json.loads(benchmark_file.read_text())
        fhir_version = benchmark["fhirVersion"]
        dataset = benchmark["dataset"]
        if size not in dataset["sizes"]:
            raise click.BadParameter(
                f"Size '{size}' is not declared for dataset '{dataset['name']}'.",
                param_hint="--size",
            )
        iterations = benchmark.get("iterations", {})
        warmup = iterations.get("warmup", DEFAULT_WARMUP)
        measurement = iterations.get("measurement", DEFAULT_MEASUREMENT)

        data_dir = locate_data_dir(data_root, dataset["name"], dataset["version"], size)
        log.info("Located materialized data at %s", data_dir)

        checkfile = load_checkfile(benchmark_file)
        _verify_dataset_integrity(data_dir, checkfile, size)

        pc = PathlingContext.create()
        cores = os.cpu_count() or 1
        # Keep the tiny SoF inputs from spawning ~200 shuffle part-files and adding scheduling noise.
        pc.spark.conf.set("spark.sql.shuffle.partitions", str(cores))

        out_dir = tempfile.mkdtemp(prefix="sof-benchmark-out-")
        try:
            ndjson = pc.read.ndjson(str(data_dir))
            results = run_cases(
                benchmark["cases"],
                size,
                checkfile,
                ndjson,
                pc,
                sink,
                out_dir,
                warmup,
                measurement,
            )

            pathling_version = resolve_version()
            report = build_report(
                pathling_version,
                pathling_version,
                benchmark,
                dataset,
                environment(pc),
                schema_sink(sink),
                warmup,
                measurement,
                size,
                fhir_version,
                resource_counts(checkfile, size),
                results,
            )
            Path(out).write_text(json.dumps(report, indent=2))
            log.info("Wrote benchmark report to %s", out)
        finally:
            pc.spark.stop()

    run()


def run_cases(
    cases: List[dict],
    size: str,
    checkfile: Optional[dict],
    ndjson,
    pc,
    sink: str,
    out_dir: str,
    warmup: int,
    measurement: int,
) -> List[dict]:
    """Loads each distinct subject once then measures every case over its loaded subject.

    :param cases: the benchmark cases in file order.
    :param size: the size key (used to resolve the checkfile assertion).
    :param checkfile: the parsed checkfile, or None.
    :param ndjson: the NDJSON-backed data source.
    :param pc: the Pathling context.
    :param sink: the Spark write format.
    :param out_dir: the fixed output directory the results are written to.
    :param warmup: the number of warmup iterations to discard.
    :param measurement: the number of measured iterations to record.
    :return: the per-case result dicts in file order.
    """
    loaded_subjects: Dict[str, dict] = {}
    results = []
    for case in cases:
        subject = case["view"]["resource"]
        if subject not in loaded_subjects:
            loaded_subjects[subject] = load_subject(ndjson, subject, pc)
        results.append(
            measure_case(
                loaded_subjects[subject],
                subject,
                case,
                expected_count(checkfile, case["id"], size),
                sink,
                out_dir,
                warmup,
                measurement,
            )
        )
    return results


def load_subject(ndjson, subject: str, pc) -> dict:
    """Performs the load phase for a subject: reads, FHIR-encodes, caches and forces materialization.

    :param ndjson: the NDJSON-backed data source.
    :param subject: the subject resource type to load.
    :param pc: the Pathling context.
    :return: a dict with the cached data source, input row count and load duration in ms.
    """
    log.info("Loading subject %s", subject)
    start = time.perf_counter()
    # DataSource.read returns the lazy FHIR-encoded DataFrame; caching plus count() forces the
    # parse+encode to happen now so the timed region measures only view evaluation and write.
    encoded = ndjson.read(subject).cache()
    input_rows = encoded.count()
    load_ms = (time.perf_counter() - start) * 1000.0

    # Rebuild as a dataset source over the already-cached DataFrame so view(...) is available.
    cached = pc.read.datasets({subject: encoded})
    log.info(
        "Loaded subject %s: %s input rows in %.1f ms", subject, input_rows, load_ms
    )
    return {"source": cached, "input_rows": input_rows, "load_ms": load_ms}


def measure_case(
    loaded: dict,
    subject: str,
    case: dict,
    expected: Optional[int],
    sink: str,
    out_dir: str,
    warmup: int,
    measurement: int,
) -> dict:
    """Measures a single case over its already-loaded subject.

    Runs the discarded warmup iterations, then the measured iterations timing each
    execute+write, and finally obtains the output row count outside the timed region.

    :param loaded: the cached subject the case runs over.
    :param subject: the subject resource type.
    :param case: the benchmark case.
    :param expected: the checkfile assertion for this case and size, if any.
    :param sink: the Spark write format.
    :param out_dir: the fixed output directory.
    :param warmup: the number of warmup iterations to discard.
    :param measurement: the number of measured iterations to record.
    :return: the measured case result dict.
    """
    view_json = json.dumps(case["view"])
    source = loaded["source"]
    log.info(
        "Measuring case '%s' (%s warmup, %s measured)", case["id"], warmup, measurement
    )

    for _ in range(warmup):
        df = source.view(json=view_json)
        df.write.format(sink).mode("overwrite").save(out_dir)

    samples: List[float] = []
    last_df = None
    for _ in range(measurement):
        df = source.view(json=view_json)
        start = time.perf_counter()
        df.write.format(sink).mode("overwrite").save(out_dir)
        samples.append((time.perf_counter() - start) * 1000.0)
        last_df = df

    # Untimed correctness guard: count() never times the measured region because Catalyst would
    # prune projected columns and under-measure.
    output_rows = last_df.count() if last_df is not None else 0
    status = correctness_status(
        expected, output_rows, bool(case.get("countVariancePermitted", False))
    )
    log.info("Case '%s': %s output rows, status %s", case["id"], output_rows, status)

    return {
        "id": case["id"],
        "status": status,
        "inputRows": loaded["input_rows"],
        "outputRows": output_rows,
        "samplesMs": samples,
        "stats": stats_for(samples),
        "phaseSamplesMs": {"load": [loaded["load_ms"]], "executeExtract": samples},
    }


if __name__ == "__main__":
    main()
