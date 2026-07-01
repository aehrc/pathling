## 1. Contract sync and test fixtures

- [x] 1.1 Confirm the `sql-on-fhir` submodule pointer is at `343f398` and stage the bump with this change.
- [x] 1.2 Add small test fixtures under `sof-benchmark/src/test/resources`: a v2 benchmark file (with suite `name`/`version`, case `id`s, one `countVariancePermitted` case) and its sibling `.check.json` (dataset identity, `sizes[size].{resourceCounts,files.sha256}`, `assertions[id][size]`), plus a tiny materialized `<name>/<version>/<size>/` NDJSON tree matching the checkfile.
- [x] 1.3 Vendor/locate the v2 `benchmark-report.schema.json` for report-validation tests.

## 2. Benchmark-file and checkfile model (Java, test-first)

- [x] 2.1 Write failing tests: `BenchmarkFile` parses suite `name`/`version`; `BenchmarkCase` parses `id` and `countVariancePermitted`; inline `expectCount` is no longer read.
- [x] 2.2 Update `BenchmarkFile`/`BenchmarkCase`/`BenchmarkDataset`: add suite `name`+`version`, case `id`+`countVariancePermitted`, dataset `version`+`syntheaVersion`; remove `expectCount` from the model.
- [x] 2.3 Write failing tests for a new `Checkfile` model: resolves `<base>.check.json`, exposes `assertions[id][size]`, per-size `resourceCounts`, and per-file `sha256`; handles an absent checkfile.
- [x] 2.4 Implement the `Checkfile` model and sibling-path resolver.

## 3. Deterministic data location and integrity (Java, test-first)

- [x] 3.1 Write failing tests: data resolves at `<dataRoot>/<name>/<version>/<size>/`; a missing directory yields a clear `(name, version, size)` error.
- [x] 3.2 Replace `ManifestLocator` with deterministic path resolution; delete the manifest-scan-by-population code.
- [x] 3.3 Write failing tests for sha256 verification: matching lock proceeds; drift or a missing locked file is reported and the run is not presented as verified.
- [x] 3.4 Implement sha256 verification of each loaded `<ResourceType>.ndjson` against the checkfile, outside the timed region.

## 4. Measurement and correctness (Java, test-first)

- [x] 4.1 Write failing tests: expected count comes from `assertions[id][size]`; present+equal ⇒ `ok`, present+unequal ⇒ `count_mismatch`, absent ⇒ `ok`; `countVariancePermitted` ⇒ always `ok`.
- [x] 4.2 Update `MeasurementHarness`/`CaseResult` to source expectations from the checkfile by case `id` and honour `countVariancePermitted`; keep csv sink, load excluded from the timed region (the `preloaded_repeated` scenario).

## 5. Report writer v2 (Java, test-first)

- [x] 5.1 Write a failing test asserting the emitted report validates against `benchmark-report.schema.json`.
- [x] 5.2 Reshape `ReportWriter`: structured `implementation{engine:Pathling, binding:pathling-java}`, `benchmark{name,version}` from the suite, `dataset{name,version}`, `measurement.scenario=preloaded_repeated` + `sink=csv`, `stats={mean,stddev,min,max,median}`, per-size `resourceCounts`, `results` keyed by suite `name`, per-case keyed by `id`; remove `benchmarkVersion` and the git-rev-parse helper and the flat `implementation.name/version`.
- [x] 5.3 Wire `SofBenchmarkRunner` to the deterministic locator, checkfile, and reshaped report; remove `ManifestLocator` usage.

## 6. Python runner parity (test-first)

- [x] 6.1 Write failing pytest cases mirroring sections 2–5 (deterministic location, checkfile assertions by `id`, `countVariancePermitted`, sha256 verification, v2 report shape).
- [x] 6.2 Update `python/sof_runner.py`: deterministic path resolution, checkfile parsing + sha256 verification, checkfile-sourced expectations, and the v2 report shape with `implementation.binding = pathling-python`.
- [x] 6.3 Add a parity assertion (Java vs Python report structure differs only by `implementation.binding`, environment and timings).

## 7. Docs and verification

- [x] 7.1 Update `sof-benchmark/python/README.md` and any runner usage notes referencing the old `<name>_<hash>` layout or inline `expectCount`.
- [x] 7.2 Run the Java build/tests (`mvn -pl sof-benchmark -am test`) and the Python runner tests; confirm both emit schema-valid reports.
- [x] 7.3 `openspec validate update-sof-benchmark-contract-v2 --strict` passes.
