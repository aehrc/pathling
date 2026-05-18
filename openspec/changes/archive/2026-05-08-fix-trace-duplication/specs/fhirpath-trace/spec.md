## MODIFIED Requirements

### Requirement: trace entry count matches logical invocations

A single source-level `trace(name [, projection])` call SHALL produce a
number of `TraceCollector` entries equal to the number of logical
invocations of that trace, irrespective of how downstream FHIRPath
operations consume the traced column. In particular, operations that
internally compile into Spark expressions referencing the traced column
more than once (for example `count()`, `exists()`, `empty()`,
`combine()`, `single()`, `iif()`, and the `|` union operator) SHALL
NOT inflate the number of collector entries.

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

#### Scenario: trace consumed by count comparison does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.count() > 0`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace before where-then-first does not duplicate entries

- **GIVEN** a Patient with three `name` entries (one with `use = 'official'`)
- **WHEN** evaluating `name.trace('t').where(use = 'official').given.first()`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

#### Scenario: trace consumed by combine does not duplicate entries

- **GIVEN** a Patient with three `name` entries
- **WHEN** evaluating `name.trace('t').given.combine(Patient.name.family)`
- **THEN** the collector SHALL contain the same number of entries as the
  baseline pass-through case

## ADDED Requirements

### Requirement: trace cannot be used inside SQL aggregate functions

A FHIRPath expression containing `trace(name [, projection])` SHALL NOT
be used as an argument to a SQL aggregate function (`sum`, `count`,
`avg`, `min`, `max`, `collect_list`, `collect_set`, …). This is a
constraint inherited from Spark: the analyzer rejects any
`Nondeterministic` expression inside an aggregate function, raising
`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`. Pathling does
not introduce or relax this constraint; it documents it.

If aggregation is required over a value derived from a traced
expression, the user SHALL move the `trace()` call upstream of the
aggregation boundary (for example, evaluate the FHIRPath expression
without `trace()` and add the trace separately on a non-aggregated
projection of the same data).

#### Scenario: traced expression inside sum raises analyzer error

- **GIVEN** a DataFrame with a column `c` derived from a FHIRPath
  expression containing `trace()`
- **WHEN** Spark plans a query of the form `df.groupBy(...).agg(sum(c))`
- **THEN** Spark SHALL raise an analyzer error with code
  `AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`
- **AND** Pathling SHALL NOT attempt to rewrite or suppress the error

#### Scenario: trace upstream of aggregation succeeds

- **GIVEN** a DataFrame with a column `c` derived from a FHIRPath
  expression NOT containing `trace()`
- **WHEN** the user runs `df.groupBy(...).agg(sum(c))` after a separate
  `df.select(traced_column).show()` to inspect the trace
- **THEN** the aggregation SHALL succeed
- **AND** the trace output SHALL be emitted by the inspection query
