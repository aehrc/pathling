## ADDED Requirements

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

## MODIFIED Requirements

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
