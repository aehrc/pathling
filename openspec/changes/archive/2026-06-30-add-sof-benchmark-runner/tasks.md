## 1. Data materialization prerequisite (one-off, gitignored)

- [x] 1.1 Download `synthea-with-dependencies-3.2.0.jar` into `sql-on-fhir/benchmark/bin/` and create `sql-on-fhir/benchmark/tools/executors.config.json` from the sample, pointing `jar` at it.
- [x] 1.2 Run `cd sql-on-fhir/benchmark && bun install && bun run data clinical-flat.json --size s`; confirm `data/synthea-clinical_<hash>/s/{Condition,Observation}.ndjson` + `manifest.json` are produced and note the recorded per-resource counts.

## 2. Java runner — build wiring

- [x] 2.1 Create the dedicated `sof-benchmark` module (child of the root `pom.xml`) depending on `library-runtime`, `spark-sql` and `com.fasterxml.jackson.core:jackson-databind` (no version, BOM-managed), with a shade `mainClass` of `au.csiro.pathling.sof.benchmark.SofBenchmarkRunner`.
- [x] 2.2 Create the `au.csiro.pathling.sof.benchmark` package; the JMH `benchmark` module stays untouched.

## 3. Java runner — implementation

- [x] 3.1 `BenchmarkFile`: parse a benchmark `*.json` with Jackson (`readTree`), exposing `dataset` (name, sizes, resources) and `cases[]` (title, view node, `view.resource`, `expectCount[size]`); provide the view-node-to-string serialization for `.json(...)`.
- [x] 3.2 `ManifestLocator`: scan `<dataRoot>/*/<size>/manifest.json` and return the dir whose manifest matches dataset `name` + `size` + `population`; clear error when none matches.
- [x] 3.3 `MeasurementHarness`: load phase (`ds.read(subject).cache().count()` → `pc.read().datasets().dataset(...)`, capturing `inputRows` and `load` duration); warmup loop (discarded); measured loop timing `write()` to a fixed temp dir with `Overwrite`; untimed `count()` for `outputRows`; set `spark.sql.shuffle.partitions` low.
- [x] 3.4 `ReportWriter`: emit `benchmark-report.json` per `benchmark-report.schema.json` (implementation, environment, `measurement` with phases/sink/warmup/iterations, per-case status/inputRows/outputRows/samplesMs/stats/phaseSamplesMs).
- [x] 3.5 `SofBenchmarkRunner.main`: CLI (`<benchmarkFile> --size --data --sink csv|parquet --out`), Spark/PathlingContext setup, orchestrate per distinct subject then per case, write the report; `status` = `ok`/`count_mismatch` vs `expectCount`.

## 4. Python runner

- [x] 4.1 Confirm the Python phase-separation accessors in `lib/python/pathling/datasource.py` (`ds.read(resource)`, `pc.read.datasets().dataset(...)`); choose the fallback if the dataset-source builder is not exposed.
- [x] 4.2 Implement `sof-benchmark/python/sof_runner.py` mirroring the Java runner (`click` CLI, manifest discovery, `pc.read.ndjson`, `ds.view(json=...)`, timed `df.write...`, untimed `df.count()`), emitting the same report structure with `implementation.name = "pathling-python"`.
- [x] 4.3 Add `sof-benchmark/python/README.md` documenting how to run it (locally-built `pathling` wheel + JVM; clear `~/.ivy2*` after Java rebuilds).

## 5. End-to-end verification

- [x] 5.1 Build: `mvn install -pl library-runtime -am` then `mvn install -pl sof-benchmark -am` (and `lib/python` for Python).
- [x] 5.2 Run Java: `java -cp sof-benchmark/target/sof-benchmark-9.9.0-SNAPSHOT.jar au.csiro.pathling.sof.benchmark.SofBenchmarkRunner sql-on-fhir/benchmark/clinical-flat.json --size s --data sql-on-fhir/benchmark/data --sink csv --out report-java.json`.
- [x] 5.3 Run Python: `python sof-benchmark/python/sof_runner.py <same args> --out report-python.json`.
- [x] 5.4 Validate both reports: `cd sql-on-fhir/benchmark && bunx ajv -s benchmark-report.schema.json -d <report>` — both must be schema-valid.
- [x] 5.5 Cross-check: outputRows match between runners; compare vs `expectCount` and record any mismatch as a cross-environment-determinism data point (not a failure).

## 6. Convert predicted findings to evidenced

- [x] 6.1 From the reports, compute the startup+codegen+encode overhead fraction at `s` (F2) and the `load` fraction of total (F4), and the Java-vs-Python timing delta (binding overhead).
- [x] 6.2 Update `sql-on-fhir/docs/superpowers/specs/2026-06-30-pathling-second-implementation-findings.md` F2/F4 and the binding-overhead note with the measured numbers (PREDICTED → CONFIRMED).
- [x] 6.3 (Optional) Repeat at `--size m` for a 2-point scaling read.
