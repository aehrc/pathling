# SQL-on-FHIR benchmark runner (Python)

`sof_runner.py` runs the implementation-agnostic SQL-on-FHIR benchmark from the
`sql-on-fhir` submodule through the Pathling **Python** API. It is the
language-binding counterpart to the Java `SofBenchmarkRunner` in
`au.csiro.pathling.sof.benchmark`: same inputs, same report structure, only
`implementation.binding` (`pathling-python`) and the timing values differ — both
report the same `implementation.engine` (`Pathling`). Running both isolates the
binding-layer (py4j) overhead on top of the shared JVM engine.

## Prerequisites

- Java 21 (the Pathling Python API runs Spark on a JVM).
- A locally-built `pathling` wheel installed into your Python environment, built
  from this checkout so it matches the Java runner's engine version:

  ```bash
  mvn clean install -pl lib/python -am
  ```

  > **Ivy cache staleness:** after rebuilding any upstream Java module, clear the
  > Ivy cache before running so the Python API picks up the fresh
  > `library-runtime`, otherwise Spark may resolve a stale jar:
  >
  > ```bash
  > rm -rf ~/.ivy2*
  > ```

- Materialized benchmark data. The data is **not** checked in — generate it with
  the submodule's tool (needs the Synthea jar and a local
  `executors.config.json`):

  ```bash
  cd sql-on-fhir/benchmark
  bun install
  bun run data clinical-flat.json --size s
  ```

  This writes `data/<name>/<version>/s/{Condition,Observation}.ndjson` plus a
  `manifest.json`, keyed by the dataset's explicit `(name, version)` identity. The
  runner resolves the directory deterministically at
  `<dataRoot>/<name>/<version>/<size>/` — no manifest scan, no recipe hash — and
  verifies each loaded file against the committed sibling checkfile
  (`<benchmark>.check.json`) `sha256` lock before benchmarking.

## Run

From the repository root:

```bash
python sof-benchmark/python/sof_runner.py \
  sql-on-fhir/benchmark/clinical-flat.json \
  --size s \
  --data sql-on-fhir/benchmark/data \
  --sink csv \
  --out report-python.json
```

| Flag       | Meaning                                                       |
| ---------- | ------------------------------------------------------------- |
| positional | Path to the benchmark `*.json` file.                          |
| `--size`   | Size key to run (`s`, `m`, …) — must be declared in the file. |
| `--data`   | Root directory of materialized datasets.                      |
| `--sink`   | Spark write format for the timed region (`csv` or `parquet`). |
| `--out`    | Report output path (default `benchmark-report.json`).         |

## What it measures

- **Load phase (separate):** reads and FHIR-encodes each subject's NDJSON,
  caches it, and forces materialization with a `count()` before any timing — so
  the timed region never absorbs parse/encode cost. The load duration is recorded
  under `phaseSamplesMs.load`.
- **Execute + extract (timed):** evaluates each case's view and writes the full
  result to the sink (never a lazy count). Warmup iterations run first and are
  discarded; one sample is recorded per measured iteration under
  `phaseSamplesMs.executeExtract`.
- **Correctness guard:** the output row count (obtained outside the timed region)
  is compared to the checkfile assertion `assertions[<case id>][size]` — `ok` when
  equal or absent, otherwise `count_mismatch`. A case that declares
  `countVariancePermitted` is always `ok` (its reference resolves in a
  `where`/`forEach` position where empty-vs-null-vs-error is engine-specific). A
  mismatch is recorded, not fatal.

## Report shape

The report conforms to contract-v2 `benchmark-report.schema.json`: a structured
`implementation` (`engine` = `Pathling` plus `binding` = `pathling-python`),
`benchmark` and `dataset` identities, a `measurement` block with
`scenario: preloaded_repeated` and `sink: csv`, per-size `resourceCounts`, and a
`results` map keyed by the authored suite `name` whose cases are keyed by their
stable `id` and carry `stats` of exactly `{mean, stddev, min, max, median}`.

## Unit tests

The runner's pure logic (data location, checkfile parsing, sha256 verification,
correctness status, stats, report shape) is covered by `test_sof_runner.py`, which
runs without a Spark session:

```bash
python -m pytest sof-benchmark/python/test_sof_runner.py
```

## Validate the report

```bash
cd sql-on-fhir/benchmark
bunx ajv -s benchmark-report.schema.json -d ../../report-python.json
```
