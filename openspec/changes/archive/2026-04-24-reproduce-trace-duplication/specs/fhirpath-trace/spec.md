## ADDED Requirements

### Requirement: trace entry count matches logical invocations

A single source-level `trace(name [, projection])` call SHALL produce a
number of `TraceCollector` entries equal to the number of logical
invocations of that trace, irrespective of how downstream FHIRPath
operations consume the traced column. In particular, operations that
internally compile into Spark expressions referencing the traced column
more than once (for example `count()`, `exists()`, `empty()`,
`combine()`, and the `|` union operator) SHALL NOT inflate the number
of collector entries.

Two independent source-level `trace()` calls, even with identical
arguments, SHALL produce independent entries — this requirement governs
duplication within a single call, not deduplication across calls.

#### Scenario: trace followed by pass-through path produces baseline entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t')` with a `TraceCollector` attached
- **THEN** the collector SHALL contain exactly the baseline number of
  entries labelled `t` for a 3-element traced collection

#### Scenario: trace consumed by join produces baseline entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.join(' ')`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case above

#### Scenario: trace consumed by count does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.count()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case, NOT a multiple of it

#### Scenario: trace consumed by exists does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').exists()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace consumed by empty does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').empty()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace consumed by first does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').first()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace consumed by combine does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.join(' ').combine('X')`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace consumed by union does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.join(' ') | name.family.first()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: two independent trace calls each produce baseline entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t') | name.trace('t')`
- **THEN** the collector SHALL contain exactly twice the baseline number
  of entries labelled `t` (one set per source-level `trace()` call),
  not four times or more
