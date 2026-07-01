# sof-benchmark-runner Specification

## Purpose
TBD - created by archiving change add-sof-benchmark-runner. Update Purpose after archive.
## Requirements
### Requirement: Manifest-first data location

The runner SHALL locate a benchmark's materialized NDJSON by scanning the
`manifest.json` files beneath the configured data root and selecting the dataset
directory whose manifest matches the benchmark's dataset `name`, the requested
`size`, and the `population` declared for that size. The runner SHALL NOT require
recomputing the reference materializer's recipe hash to find the data.

#### Scenario: Matching manifest is selected

- **WHEN** the data root contains a `manifest.json` with `name`, `size` and
  `population` equal to the benchmark's dataset name, the requested size, and that
  size's declared population
- **THEN** the runner uses that manifest's directory and reads each case's
  `<view.resource>.ndjson` from it

#### Scenario: No matching manifest

- **WHEN** no manifest under the data root matches the dataset name, size and
  population
- **THEN** the runner reports a clear error identifying the unmet (name, size,
  population) rather than silently producing an empty result

### Requirement: Single-view execute-and-extract timing

For each case, the runner SHALL evaluate the case's `view` over the materialized
data for the chosen size and time a region that covers both evaluation and a full
materialization of the result to a table/csv sink (never a lazy count). The runner
SHALL run the warmup iterations before the measured iterations and SHALL discard
the warmup samples, returning one timing sample per measured iteration.

#### Scenario: Warmup discarded, one sample per measured iteration

- **WHEN** a case is timed with `warmup: 1` and `measurement: 5`
- **THEN** the report contains exactly five timing samples for that case
- **AND** none of the samples correspond to the warmup iteration

#### Scenario: Materialization is timed, not a lazy count

- **WHEN** the runner measures a case
- **THEN** the timed region writes the full result to the configured sink
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

For each case, the runner SHALL compare the output row count against
`expectCount[size]`: when the expectation is present and equal it SHALL report
`ok`; when present and unequal it SHALL report `count_mismatch`; when absent it
SHALL report `ok`. A `count_mismatch` SHALL be recorded in the report rather than
aborting the run.

#### Scenario: Matching count is ok

- **WHEN** a case's output row count equals its `expectCount[size]`
- **THEN** the case is reported as `ok`

#### Scenario: Mismatching count is flagged but does not abort

- **WHEN** a case's output row count differs from a present `expectCount[size]`
- **THEN** the case is reported as `count_mismatch`
- **AND** the runner continues measuring the remaining cases and still emits a
  report

### Requirement: Schema-valid benchmark report

The runner SHALL emit a `benchmark-report.json` that conforms to
`benchmark-report.schema.json`, including the `implementation` (name and version),
a `measurement` block declaring the timed `phases`, `sink`, and actual `warmup` and
`iterations` counts, and a `results` entry per benchmark keyed by title with `size`,
`fhirVersion`, and per-case `status`, `inputRows`, `outputRows`, timing samples and
stats.

#### Scenario: Report validates against the schema

- **WHEN** the runner completes a benchmark
- **THEN** the emitted report validates against `benchmark-report.schema.json`
- **AND** the `measurement` block records the sink and the actual warmup and
  measurement iteration counts used

### Requirement: Java and Python runner parity

The change SHALL provide two runners — a Java runner over the library-api and a
Python runner over the `pathling` package — that both implement the runner
behaviour above. Both runners MUST accept the same inputs (benchmark file, size,
data root, sink) and MUST emit reports of the same structure, distinguished only
by `implementation.name`.

#### Scenario: Equivalent inputs produce structurally equivalent reports

- **WHEN** the Java and Python runners are run over the same benchmark file, size
  and materialized data
- **THEN** both emit schema-valid reports with the same benchmark and case titles
  and the same output row counts
- **AND** the reports differ only in `implementation.name` and the timing values

