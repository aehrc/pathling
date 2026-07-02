# sof-benchmark-runner Specification

## Purpose
TBD - created by archiving change add-sof-benchmark-runner. Update Purpose after archive.
## Requirements
### Requirement: Deterministic data location by name/version/size

The runner SHALL locate a benchmark's materialized NDJSON at the deterministic path
`<dataRoot>/<dataset.name>/<dataset.version>/<size>/`, using the dataset's explicit
`name` and `version` identity from the benchmark file and the requested `size`. The
runner SHALL NOT scan `manifest.json` files nor match on `population`, and SHALL NOT
recompute the reference materializer's recipe hash.

#### Scenario: Data resolved from the identity path

- **WHEN** the benchmark declares `dataset.name`, `dataset.version` and a `size`
- **THEN** the runner reads each case's `<view.resource>.ndjson` from
  `<dataRoot>/<name>/<version>/<size>/`

#### Scenario: Missing data directory

- **WHEN** no directory exists at `<dataRoot>/<name>/<version>/<size>/`
- **THEN** the runner reports a clear error identifying the resolved path and the
  `(name, version, size)` it expected, rather than silently producing an empty result

### Requirement: Dataset integrity verification against the checkfile

The runner SHALL verify each materialized `<ResourceType>.ndjson` it loads against the
sibling checkfile's per-file `sha256` lock (`sizes[size].files[<file>].sha256`). When a
loaded file's computed sha256 does not match the locked value, or a locked file is
missing, the runner SHALL report the drift clearly and SHALL NOT report benchmark
timings as if run over the locked data.

#### Scenario: Checksum matches the lock

- **WHEN** every loaded `<ResourceType>.ndjson` sha256 equals the checkfile's locked value
  for that size
- **THEN** the runner proceeds to measure the cases

#### Scenario: Checksum drift is reported

- **WHEN** a loaded `<ResourceType>.ndjson` sha256 differs from the checkfile's locked
  value, or a locked file is absent
- **THEN** the runner reports the offending file, its locked and actual sha256, and does
  not present the run as verified against the lock

### Requirement: Single-view execute-and-extract timing

For each case, the runner SHALL evaluate the case's `view` over the materialized data for
the chosen size and time a region that covers both evaluation and a full materialization
of the result to a `csv` sink (never a lazy count). The runner SHALL run the warmup
iterations before the measured iterations and SHALL discard the warmup samples, returning
one timing sample per measured iteration. This maps to the `preloaded_repeated` measurement
scenario: the load phase is excluded from the timed region, and the sink is `csv`.

#### Scenario: Warmup discarded, one sample per measured iteration

- **WHEN** a case is timed with `warmup: 1` and `measurement: 5`
- **THEN** the report contains exactly five timing samples for that case
- **AND** none of the samples correspond to the warmup iteration

#### Scenario: Materialization to csv is timed, not a lazy count

- **WHEN** the runner measures a case
- **THEN** the timed region writes the full result to a `csv` sink
- **AND** the output row count is obtained outside the timed region

### Requirement: Load phase measured separately

The runner SHALL read and FHIR-encode the input as a distinct load phase, force
its materialization before the timed execute+extract region so that region does
not absorb parse/encode cost, and record the load duration separately in the
report.

#### Scenario: Load cost excluded from execute+extract

- **WHEN** a benchmark case is measured
- **THEN** the input has already been materialized when the timed execute+extract
  region begins
- **AND** the report records the load duration separately from the execute+extract
  samples

### Requirement: Output row-count correctness guard

For each case, the runner SHALL obtain the expected output row count from the sibling
checkfile `<benchmark>.check.json` at `assertions[case.id][size]`, keyed by the case's
stable `id`. When the expectation is present and equal it SHALL report `ok`; when present
and unequal it SHALL report `count_mismatch`; when absent it SHALL report `ok`. When a case
declares `countVariancePermitted: true`, the runner SHALL NOT report `count_mismatch` for a
count divergence and SHALL report `ok`. A `count_mismatch` SHALL be recorded in the report
rather than aborting the run.

#### Scenario: Matching checkfile assertion is ok

- **WHEN** a case's output row count equals its checkfile `assertions[id][size]`
- **THEN** the case is reported as `ok`

#### Scenario: Mismatching count is flagged but does not abort

- **WHEN** a case's output row count differs from a present `assertions[id][size]`
- **AND** the case does not declare `countVariancePermitted: true`
- **THEN** the case is reported as `count_mismatch`
- **AND** the runner continues measuring the remaining cases and still emits a report

#### Scenario: Variance-permitted case is never a mismatch

- **WHEN** a case declares `countVariancePermitted: true` and its output row count differs
  from the checkfile assertion
- **THEN** the case is reported as `ok`

### Requirement: Per-case failure isolation

The runner SHALL measure each case under its own failure boundary. When loading,
preparing, or evaluating a case raises, the runner SHALL record that case with
`status: execution_error` and an advisory free-text `message`, and SHALL CONTINUE
to the remaining cases. A failing case SHALL NOT abort the run and SHALL NOT change
the recorded status of any other case. A run in which one or more cases failed SHALL
still emit a report that validates against `benchmark-report.schema.json` and
contains every case that ran, each with its own status.

`execution_error` is the conformant status for any load/prepare/evaluate failure;
the runner is NOT required to distinguish the OPTIONAL `timeout` or `malformed`
refinements, and does NOT impose a wall-clock budget.

#### Scenario: A failing case does not abort the run

- **WHEN** one case raises while loading or evaluating and the other cases succeed
- **THEN** the failing case is recorded with `status: execution_error` and an advisory
  `message`
- **AND** the remaining cases are still measured and recorded with their own statuses
- **AND** the emitted report validates against `benchmark-report.schema.json`

#### Scenario: Failure of one case leaves others unchanged

- **WHEN** a run contains a failing case and a succeeding case
- **THEN** the succeeding case's status is unaffected by the failure
- **AND** the report contains both cases

#### Scenario: timeout and malformed are not required

- **WHEN** the runner records a load/prepare/evaluate failure
- **THEN** it records `execution_error` without being obliged to classify the failure
  as `timeout` or `malformed`

### Requirement: Schema-valid benchmark report

The runner SHALL emit a `benchmark-report.json` that conforms to
`benchmark-report.schema.json` (contract v2, Wave 2), including:
- a structured `implementation` with `engine: { name: "Pathling", version }` and a
  `binding: { name, version }` identifying the language binding (`pathling-java` or
  `pathling-python`);
- `benchmark: { name, version }` sourced directly from the authored suite `name` and
  `version`, and `dataset: { name, version }` sourced from the dataset identity;
- a `measurement` block with a required `scenario` of `preloaded_repeated`, `sink` of
  `csv`, the timed `phases`, and the actual `warmup` and `iterations` counts;
- a `results` map keyed by the authored suite `name`, each entry carrying `size`,
  `fhirVersion`, per-size `resourceCounts`, and per-case entries keyed by the case `id`
  with a `status` drawn from the taxonomy
  `{ ok, count_mismatch, generation_error, execution_error, timeout, malformed }`,
  `inputRows`, `outputRows`, timing `samplesMs`, a `stats` object of exactly
  `{ mean, stddev, min, max, median }`, and an OPTIONAL free-text `message` that is
  present only for a non-`ok` case and omitted for an `ok` case.
The runner SHALL NOT emit the removed scalar `benchmarkVersion` nor the flat
`implementation.name`/`implementation.version`.

#### Scenario: Report validates against the v2 schema

- **WHEN** the runner completes a benchmark
- **THEN** the emitted report validates against `benchmark-report.schema.json`
- **AND** `implementation.engine.name` is `Pathling` and `implementation.binding.name`
  identifies the language binding
- **AND** `benchmark.name`/`benchmark.version` equal the authored suite identity and the
  `results` map is keyed by that suite `name`

#### Scenario: Stats object carries exactly the fixed key set

- **WHEN** a case's stats are recorded
- **THEN** the `stats` object contains exactly `mean`, `stddev`, `min`, `max` and `median`
  and no other keys

#### Scenario: Message is present only for a non-ok case

- **WHEN** a case is reported as `ok`
- **THEN** its entry omits the `message` field
- **WHEN** a case is reported with a non-`ok` status such as `execution_error`
- **THEN** its entry MAY carry an advisory `message` and the report still validates

### Requirement: Java and Python runner parity

The change SHALL provide two runners — a Java runner over the library-api and a Python
runner over the `pathling` package — that both implement the runner behaviour above. Both
runners MUST accept the same inputs (benchmark file, size, data root, sink) and MUST emit
reports of the same structure, distinguished only by `implementation.binding` and the
timing values.

#### Scenario: Equivalent inputs produce structurally equivalent reports

- **WHEN** the Java and Python runners are run over the same benchmark file, size and
  materialized data
- **THEN** both emit schema-valid reports keyed by the same suite `name`, with the same
  case `id`s and the same output row counts
- **AND** the reports differ only in `implementation.binding`, the environment, and the
  timing values

