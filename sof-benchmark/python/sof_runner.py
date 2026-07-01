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

"""SQL-on-FHIR benchmark runner over the Pathling Python API.

This mirrors the Java ``SofBenchmarkRunner``: it locates a benchmark's materialized
NDJSON manifest-first, separates the FHIR load/encode phase from the timed
execute+extract region, times a full write of each case's result (never a lazy
count), checks the output row count against ``expectCount``, and emits a report
conforming to ``benchmark-report.schema.json`` with
``implementation.name = "pathling-python"``.
"""

import json
import logging
import os
import platform
import statistics
import subprocess
import tempfile
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Dict, List, Optional

import click
from pathling import PathlingContext
from pathling.datasource import DataSource

IMPLEMENTATION_NAME = "pathling-python"
DEFAULT_SINK = "csv"
DEFAULT_OUT = "benchmark-report.json"
DEFAULT_WARMUP = 1
DEFAULT_MEASUREMENT = 5

# The sink categories permitted by ``benchmark-report.schema.json``.
SCHEMA_SINKS = frozenset({"table", "csv", "memory", "other"})

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("sof_runner")


def locate_data_dir(data_root: Path, name: str, size: str, population: int) -> Path:
    """Locates the size-specific data directory by matching manifests.

    Scans ``<data_root>/*/<size>/manifest.json`` and returns the directory whose manifest
    matches the dataset name, size and population, so the reference materializer's recipe
    hash is never reproduced.

    :param data_root: the root directory containing materialized datasets.
    :param name: the dataset name to match.
    :param size: the size key to match.
    :param population: the population to match.
    :return: the directory containing the matching size's NDJSON files and manifest.
    :raises FileNotFoundError: if no manifest under the data root matches all three.
    """
    if not data_root.is_dir():
        raise FileNotFoundError(
            f"Data root does not exist or is not a directory: {data_root}"
        )
    for dataset_dir in sorted(data_root.iterdir()):
        manifest_path = dataset_dir / size / "manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text())
        if (
            manifest.get("name") == name
            and manifest.get("size") == size
            and manifest.get("population") == population
        ):
            return manifest_path.parent
    raise FileNotFoundError(
        f"No materialized dataset matches (name={name}, size={size}, population={population}) "
        f"under {data_root}. Run the materializer first: "
        f"cd sql-on-fhir/benchmark && bun run data <file> --size {size}"
    )


def schema_sink(sink: str) -> str:
    """Maps a Spark write format to a sink category permitted by the report schema.

    File formats such as ``parquet`` that are not enumerated by the schema are reported
    as ``other`` so the report stays schema-valid.

    :param sink: the Spark write format used for the timed region.
    :return: a schema-valid sink category.
    """
    return sink if sink in SCHEMA_SINKS else "other"


def stats_for(samples: List[float]) -> Dict[str, float]:
    """Computes summary statistics for a list of timing samples.

    :param samples: the timing samples in milliseconds.
    :return: a dict with min, mean, median and max (empty if there are no samples).
    """
    if not samples:
        return {}
    return {
        "min": min(samples),
        "mean": statistics.fmean(samples),
        "median": statistics.median(samples),
        "max": max(samples),
    }


def resolve_benchmark_version(benchmark_file: Path) -> Optional[str]:
    """Best-effort resolution of the benchmark submodule git SHA.

    :param benchmark_file: the path to the benchmark file.
    :return: the git SHA of the benchmark file's directory, or None if unavailable.
    """
    directory = benchmark_file.resolve().parent
    try:
        result = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha if result.returncode == 0 and sha else None


def resolve_version() -> str:
    """Resolves the installed Pathling package version.

    :return: the Pathling version, or "UNKNOWN" if it cannot be determined.
    """
    try:
        return version("pathling")
    except PackageNotFoundError:
        return "UNKNOWN"


def environment(pc: PathlingContext) -> Dict[str, object]:
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
def main(
    benchmark_file: Path, size: str, data_root: Path, sink: str, out: Path
) -> None:
    """Run the SQL-on-FHIR benchmark with the Pathling Python API."""
    benchmark = json.loads(benchmark_file.read_text())
    title = benchmark["title"]
    fhir_version = benchmark["fhirVersion"]
    dataset = benchmark["dataset"]
    name = dataset["name"]
    if size not in dataset["sizes"]:
        raise click.BadParameter(
            f"Size '{size}' is not declared for dataset '{name}'.", param_hint="--size"
        )
    population = dataset["sizes"][size]["population"]
    iterations = benchmark.get("iterations", {})
    warmup = iterations.get("warmup", DEFAULT_WARMUP)
    measurement = iterations.get("measurement", DEFAULT_MEASUREMENT)

    data_dir = locate_data_dir(data_root, name, size, population)
    log.info("Located materialized data at %s", data_dir)

    pc = PathlingContext.create()
    cores = os.cpu_count() or 1
    # Keep the tiny SoF inputs from spawning ~200 shuffle part-files and adding scheduling noise.
    pc.spark.conf.set("spark.sql.shuffle.partitions", str(cores))

    out_dir = tempfile.mkdtemp(prefix="sof-benchmark-out-")
    try:
        ndjson = pc.read.ndjson(str(data_dir))
        results = run_cases(
            benchmark["cases"], size, ndjson, pc, sink, out_dir, warmup, measurement
        )

        report = {
            "implementation": {
                "name": IMPLEMENTATION_NAME,
                "version": resolve_version(),
            },
            "environment": environment(pc),
            "measurement": {
                "phases": ["execute", "extract"],
                "sink": schema_sink(sink),
                "warmup": warmup,
                "iterations": measurement,
            },
            "results": {
                title: {"size": size, "fhirVersion": fhir_version, "cases": results}
            },
        }
        benchmark_version = resolve_benchmark_version(benchmark_file)
        if benchmark_version:
            report["benchmarkVersion"] = benchmark_version

        Path(out).write_text(json.dumps(report, indent=2))
        log.info("Wrote benchmark report to %s", out)
    finally:
        pc.spark.stop()


def run_cases(
    cases: List[dict],
    size: str,
    ndjson: DataSource,
    pc: PathlingContext,
    sink: str,
    out_dir: str,
    warmup: int,
    measurement: int,
) -> List[dict]:
    """Loads each distinct subject once then measures every case over its loaded subject.

    :param cases: the benchmark cases in file order.
    :param size: the size key (used to resolve expectCount).
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
                size,
                sink,
                out_dir,
                warmup,
                measurement,
            )
        )
    return results


def load_subject(ndjson: DataSource, subject: str, pc: PathlingContext) -> dict:
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
    log.info("Loaded subject %s: %s input rows in %.1f ms", subject, input_rows, load_ms)
    return {"source": cached, "input_rows": input_rows, "load_ms": load_ms}


def measure_case(
    loaded: dict,
    subject: str,
    case: dict,
    size: str,
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
    :param size: the size key (used to resolve expectCount).
    :param sink: the Spark write format.
    :param out_dir: the fixed output directory.
    :param warmup: the number of warmup iterations to discard.
    :param measurement: the number of measured iterations to record.
    :return: the measured case result dict.
    """
    view_json = json.dumps(case["view"])
    source = loaded["source"]
    log.info(
        "Measuring case '%s' (%s warmup, %s measured)",
        case["title"],
        warmup,
        measurement,
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
    expect = case.get("expectCount", {}).get(size)
    status = "ok" if expect is None or expect == output_rows else "count_mismatch"
    log.info(
        "Case '%s': %s output rows, status %s", case["title"], output_rows, status
    )

    return {
        "title": case["title"],
        "status": status,
        "inputRows": loaded["input_rows"],
        "outputRows": output_rows,
        "samplesMs": samples,
        "stats": stats_for(samples),
        "phaseSamplesMs": {"load": [loaded["load_ms"]], "executeExtract": samples},
    }


if __name__ == "__main__":
    main()
