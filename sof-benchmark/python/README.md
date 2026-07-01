# SQL-on-FHIR benchmark runner (Python)

`sof_runner.py` runs the implementation-agnostic SQL-on-FHIR benchmark from the
`sql-on-fhir` submodule through the Pathling **Python** API. It is the
language-binding counterpart to the Java `SofBenchmarkRunner` in
`au.csiro.pathling.sof.benchmark`: same inputs, same report structure, only
`implementation.name` (`pathling-python`) and the timing values differ. Running
both isolates the binding-layer (py4j) overhead on top of the shared JVM engine.

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

  This writes `data/<name>_<hash>/s/{Condition,Observation}.ndjson` plus a
  `manifest.json`. The runner locates the data manifest-first (matching dataset
  `name` + `size` + `population`), so you never reproduce the materializer's
  recipe hash.

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
  is compared to `expectCount[size]` — `ok` when equal or absent, otherwise
  `count_mismatch`. A mismatch is recorded, not fatal.

## Validate the report

```bash
cd sql-on-fhir/benchmark
bunx ajv -s benchmark-report.schema.json -d ../../report-python.json
```
