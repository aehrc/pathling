## Context

The `sof-benchmark` module runs the implementation-agnostic SQL-on-FHIR benchmark over
Pathling, in two parity runners (Java over library-api, Python over the `pathling`
package). It was built against the pre-v2 benchmark contract. The `sql-on-fhir` submodule
has since advanced to contract v2 (`343f398`, incorporating the Pathling-implementer
feedback in issue #18), which:

- keys datasets by explicit `(name, version)` and materializes to
  `data/<name>/<version>/<size>/` (was `data/<name>_<hash>/<size>/`);
- moves expected row counts and dataset checksums out of the benchmark file into a
  committed sibling checkfile `<benchmark>.check.json`;
- gives each case a stable `id` and adds `countVariancePermitted`;
- adds an authored suite `name` + `version` to the benchmark file;
- reshapes the report (structured implementation identity, benchmark/dataset provenance,
  required `scenario` with `sink: csv`, a fixed `stats` set, per-size `resourceCounts`,
  results keyed by suite `name`).

The current runner therefore fails on v2 data at four points: it cannot find the data,
it silently loses its correctness check (no inline `expectCount`), and its report is
rejected by the v2 schema (missing `scenario`; `stats.median` forbidden and
`stddev/p50/p95` required; case `title` where `id` is required).

Constraints: Java code follows the module conventions (no inner classes/records/enums,
`final`, nullability annotations, functional style); Python follows PEP 8 with type hints
and pytest. The two runners must stay behaviourally in lockstep.

## Goals / Non-Goals

**Goals:**
- Bring both runners onto contract v2 so a v2 benchmark + checkfile runs end-to-end and
  emits a report that validates against `benchmark-report.schema.json`.
- Source correctness expectations from the checkfile keyed by case `id`; honour
  `countVariancePermitted`.
- Verify loaded NDJSON against the checkfile `sha256` lock.
- Keep Java/Python parity: same inputs, structurally identical reports differing only by
  `implementation.binding`, environment, and timings.
- Bump the `sql-on-fhir` submodule pointer to `343f398` as part of this change.

**Non-Goals:**
- No change to what is measured (preloaded, load excluded, csv extract timed) beyond
  labelling it the `preloaded_repeated` scenario with a `csv` sink.
- No materialization tooling in this module — data is produced by the submodule's `bun`
  materializer; the runner only consumes it.
- No runtime/backend classification of the engine (tracked separately in sql-on-fhir #19).
- No JMH-visualizer export (sql-on-fhir #11) and no multi-resource / referenced datasets.

## Decisions

### 1. Replace `ManifestLocator` with deterministic path resolution
Resolve `<dataRoot>/<name>/<version>/<size>/` directly from the benchmark's dataset
identity. `BenchmarkDataset` gains `version` (and `syntheaVersion`, parsed for completeness
though only `version` participates in the path). The manifest-scan-by-population code is
deleted in both runners.
- *Alternative considered*: keep scanning manifests and additionally match `version`.
  Rejected — the path is now fully determined by identity, so a scan is dead weight and a
  source of the F1/F6 fragility the spec change set out to remove.

### 2. Checkfile as the source of correctness facts
Resolve the sibling checkfile by swapping the benchmark file's extension
(`<base>.json` → `<base>.check.json`), matching the submodule's `checkfileFor()`. Parse
`dataset{name,version}`, `sizes[size].files[*].sha256`, `sizes[size].resourceCounts`, and
`assertions[caseId][size]`. Expected counts come from `assertions`; `resourceCounts` feed
the report; `files[*].sha256` drives integrity verification.
- The checkfile is required for a correctness-checked run. If it is absent, the runner
  proceeds but records `ok` for every case (expectation absent) — consistent with the
  existing "expectation absent ⇒ ok" rule — and skips sha256 verification with a clear log.

### 3. sha256 verification of loaded NDJSON
Before (or as part of) the load phase, compute each loaded `<ResourceType>.ndjson`'s
sha256 and compare to the checkfile lock. Drift or a missing locked file is reported
clearly and the run is not presented as verified against the lock. Verification is outside
the timed region.
- *Alternative considered*: trust the path and skip hashing. Rejected — cheap insurance
  that the two engines are compared over byte-identical data, which is the whole point of
  the TZ=UTC byte-identity work upstream.

### 4. Structured report identity
`implementation = { engine: { name: "Pathling", version: <coreVersion> }, binding: { name:
"pathling-java"|"pathling-python", version: <bindingVersion> } }`. `benchmark = { name,
version }` from the authored suite fields; `dataset = { name, version }` from the dataset.
The scalar `benchmarkVersion` (git SHA) and the git-rev-parse helper are removed. `results`
is keyed by the suite `name`; per-case entries key on `id`.
- Engine version = the Pathling core build version (as today via `PathlingVersion` in Java
  / package version in Python). Binding version is the same value for now (the binding ships
  with core); kept as a distinct field so it can diverge later.

### 5. Fixed stats set and scenario/sink
`stats = { mean, stddev, min, max, median }` exactly (drop the old `median`-plus-min/mean/max
shape's incompatibility by adding `stddev` and removing everything else). `measurement`
gains `scenario: "preloaded_repeated"` and keeps `sink: "csv"`; `phases` stays
`["execute", "extract"]` with `load` recorded in `phaseSamplesMs`. Raw `samplesMs` remain so
consumers can recompute richer percentiles.

### 6. Model/parsing shape
`BenchmarkCase` gains `id` and `countVariancePermitted`, and drops inline `expectCount`
(now sourced from the checkfile at measure time). `BenchmarkFile` gains suite `name` and
`version`. A new small checkfile model (Java class + Python dict access) is introduced;
`ManifestLocator` is removed. `ReportWriter`/`sof_runner` report builders are reshaped per
decisions 4–5.

## Risks / Trade-offs

- **Silent "all ok" when the checkfile is missing** → Mitigation: log prominently that no
  checkfile was found and that correctness is unverified; sha256 verification is skipped
  with an explicit message. The behaviour matches the existing absent-expectation rule, so
  it is not a regression.
- **Java/Python drift during the reshape** → Mitigation: implement the report shape from
  the same decision list, and keep a parity test asserting the two reports differ only in
  `implementation.binding`, environment and timings.
- **Submodule bump couples this change to `343f398`** → Mitigation: the pointer bump is part
  of the change and the schemas are vendored via the submodule, so the contract the runner
  targets is pinned and reviewable in the same PR.
- **sha256 cost on large sizes** → Mitigation: hashing is linear in file bytes and runs once
  per subject outside the timed region; negligible next to Spark load/encode.

## Migration Plan

1. Bump the `sql-on-fhir` submodule pointer to `343f398` (already staged in the working tree).
2. Update the Java runner (model, deterministic locator, checkfile model + sha256 verifier,
   measurement scenario/sink, report writer) test-first.
3. Mirror the Python runner.
4. Update `sof-benchmark/python/README.md` and any usage docs referencing the old layout /
   `expectCount`.
5. Validate emitted reports against the vendored `benchmark-report.schema.json`.

No production deployment; the module is a developer/CI benchmark runner. Rollback is
reverting the branch and the submodule pointer.

## Open Questions

- None blocking. Binding-vs-engine version may diverge in future (see decision 4); left as
  separate fields to accommodate that without a further schema change.
