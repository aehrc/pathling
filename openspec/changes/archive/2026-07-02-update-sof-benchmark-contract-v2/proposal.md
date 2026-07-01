## Why

The SQL-on-FHIR benchmark contract the `sof-benchmark` runner targets has advanced
to contract v2 (submodule `343f398`), which splits authored intent (the benchmark
file) from generated facts (a committed sibling checkfile) and reshapes the report.
The current runner was built against the pre-v2 contract, so against v2 data it can
no longer locate the data, silently loses its correctness check, and emits a report
that fails `benchmark-report.schema.json` validation.

## What Changes

- **BREAKING** Data location moves from scanning `manifest.json` by `(name, size, population)`
  to resolving the deterministic path `data/<name>/<version>/<size>/`, using the dataset's
  explicit `version` identity.
- **BREAKING** Expected output row counts are sourced from the sibling checkfile
  `<benchmark>.check.json` (`assertions[caseId][size]`) instead of the removed inline
  `case.expectCount`; cases are keyed by their stable `id`.
- Honour `countVariancePermitted` on a case: the runner MUST NOT flag a count divergence
  as `count_mismatch` for such cases (row-count invariance holds only for projection-position
  references).
- Verify the loaded NDJSON against the checkfile's per-file `sha256` lock, reporting drift
  clearly rather than benchmarking stale or wrong data.
- **BREAKING** Report reshaping to the v2 report schema:
  - `implementation` becomes `{ engine: { name: "Pathling", version }, binding: { name, version } }`
    (`pathling-java` / `pathling-python`); the flat `implementation.name/version` is removed.
  - Add `benchmark { name, version }` (from the authored suite `name`/`version`) and
    `dataset { name, version }`; remove the scalar `benchmarkVersion` git SHA.
  - `results` map keyed by the authored suite `name` (not the mutable `title`); per-case
    entries keyed by `id` (not `title`).
  - `measurement.scenario` is required (`preloaded_repeated`) and `sink` is `csv`.
  - `stats` is exactly `{ mean, stddev, min, max, median }`.
  - Add per-size `resourceCounts` to each results entry.
- Runner parity is now distinguished by `implementation.binding`, not `implementation.name`.

## Capabilities

### New Capabilities
<!-- None. This change modifies the existing sof-benchmark-runner capability. -->

### Modified Capabilities
- `sof-benchmark-runner`: data location becomes deterministic by `name/version/size`;
  correctness expectations move to the checkfile and are keyed by case `id`, with
  `countVariancePermitted` honoured; a new dataset-integrity (sha256) verification is
  added; and the emitted report adopts the v2 report schema (structured implementation
  identity, benchmark/dataset provenance, required scenario with csv sink, fixed stats
  set, per-size resourceCounts, and results keyed by suite name).

## Impact

- **Code (Java)**: `BenchmarkFile`, `BenchmarkCase`, `BenchmarkDataset`, `ManifestLocator`
  (replaced by deterministic resolution), `MeasurementHarness`, `CaseResult`, `ReportWriter`,
  `SofBenchmarkRunner`; new checkfile model + sha256 verifier.
- **Code (Python)**: `python/sof_runner.py` (all of the above, mirrored).
- **Contract**: consumes `benchmark.schema.json`, `benchmark-checkfile.schema.json`, and
  `benchmark-report.schema.json` from the `sql-on-fhir` submodule at `343f398` (submodule
  pointer bump accompanies this change).
- **Docs**: `sof-benchmark/python/README.md` and any runner usage notes referencing the old
  layout / `expectCount`.
- No change to the core Pathling library API; this is confined to the `sof-benchmark` module.
