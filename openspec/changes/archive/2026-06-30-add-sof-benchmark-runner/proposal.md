## Why

The `sql-on-fhir` submodule defines an implementation-agnostic SQL-on-FHIR
performance benchmark (a Synthea dataset recipe + ViewDefinition cases +
per-size expected row counts), but so far it has been exercised only by its own
Bun/JS reference runner — meaning the spec and its sole implementation
co-evolved and share each other's blind spots. Building Pathling (JVM + Apache
Spark) as an independent second runner both stress-tests that benchmark spec
from a radically different engine and gives Pathling a reusable, reportable
SQL-on-FHIR performance harness.

## What Changes

- Add a **Java benchmark runner** in a dedicated `sof-benchmark` module (new
  `au.csiro.pathling.sof.benchmark` package) that reads a benchmark file, locates
  its materialized NDJSON, executes each `case.view` through the Pathling
  library-api, times execute+extract to a table/csv sink, checks output row
  counts against `expectCount`, and emits a `benchmark-report.json`.
- Add a **Python benchmark runner** (`sof-benchmark/python/sof_runner.py`) that
  mirrors the Java runner over the `pathling` package, so the same workload can
  be measured through the language bindings — isolating binding-layer overhead.
- **Locate data manifest-first**: discover the materialized dataset directory by
  matching `manifest.json` on dataset `name` + `size` + `population`, rather than
  recomputing the reference runner's recipe hash. This deliberately demonstrates
  the fix proposed in the second-implementation findings (the "any-language"
  contract should not require reproducing an undocumented JS canonicalization).
- **Separate the load phase** (NDJSON read + FHIR encode) from the timed
  execute+extract region, so the report exposes the columnar-ingest cost that an
  in-memory engine hides.
- Leave the existing JMH microbenchmark in `benchmark/` **untouched**; the new
  runner lives in its own `sof-benchmark` module with its own shaded `mainClass`,
  so the two benchmarks share no code and no build wiring.
- Record the resulting measurements back into the findings document to convert
  its predicted spec weaknesses (startup overhead dominating small size tiers;
  load being the hidden cost; Java-vs-Python binding overhead) into confirmed,
  evidenced findings.

## Capabilities

### New Capabilities

- `sof-benchmark-runner`: Running the implementation-agnostic SQL-on-FHIR
  benchmark with Pathling. Covers manifest-first data location, single-view
  execute+extract timing with warmup discarded, separate load-phase measurement,
  the output row-count correctness guard (`ok` / `count_mismatch`), schema-valid
  `benchmark-report.json` emission, and parity between the Java and Python
  runners.

### Modified Capabilities

<!-- None. No existing Pathling capability's requirements change; the JMH
     microbenchmark is untouched and the SQL-on-FHIR spec files are not modified. -->

## Impact

- **New code**: `sof-benchmark/src/main/java/au/csiro/pathling/sof/benchmark/`
  (`SofBenchmarkRunner`, `ManifestLocator`, `MeasurementHarness`, `ReportWriter`,
  `BenchmarkFile`); `sof-benchmark/python/sof_runner.py` + a short `README.md`.
- **Build**: a new `sof-benchmark` module (child of the root `pom.xml`) depending
  on `library-runtime`, `spark-sql` and
  `com.fasterxml.jackson.core:jackson-databind` (version managed by the imported
  `jackson-bom`), shaded with its own `SofBenchmarkRunner` `mainClass`.
- **Consumes** (read-only) the library-api surface: `PathlingContext`,
  `QueryableDataSource.read(String)` / `view(...).json(...).execute()`,
  `DataSourceBuilder.datasets()`, and standard Spark `Dataset.write()`.
- **Prerequisite, not code**: materialized data from the submodule's
  `bun run data` tool, which needs `synthea-with-dependencies-3.2.0.jar` and a
  local `executors.config.json` (both gitignored).
- **Out of scope**: no changes to the JMH module, no changes to the `sql-on-fhir`
  spec/tooling (improvements there remain written-up proposals), no publishing of
  Pathling on the implementations page.
