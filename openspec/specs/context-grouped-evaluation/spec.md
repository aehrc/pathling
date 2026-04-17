## ADDED Requirements

### Requirement: ResultGroup data structure

The evaluation result SHALL use a `ResultGroup` class to represent the results
and traces for a single evaluation scope. Each `ResultGroup` SHALL contain:

- A nullable `contextKey` (String) identifying the context element.
- A `results` list of typed values (type name + materialised value).
- A `traces` list of trace results (label + typed values).

`SingleInstanceEvaluationResult` SHALL contain a `resultGroups` field
(List\<ResultGroup\>) and an `expectedReturnType` field (String). The previous
top-level `results` and `traces` fields SHALL be removed.

#### Scenario: Non-context evaluation produces single ResultGroup

- **WHEN** a FHIRPath expression is evaluated without a context expression
- **THEN** the result SHALL contain exactly one `ResultGroup` with
  `contextKey` set to null, and the group's `results` and `traces` SHALL
  contain all evaluation output

#### Scenario: Context evaluation produces multiple ResultGroups

- **WHEN** a FHIRPath expression is evaluated with context expression `name`
  against a Patient with 3 name entries
- **THEN** the result SHALL contain 3 `ResultGroup` entries, one per context
  element, each with its own `results` and `traces`

### Requirement: Context key format

When a context expression is provided, each `ResultGroup` SHALL have a
`contextKey` in the format `contextExpression[index]`, where `index` is the
zero-based position of the element in the context array.

When the context expression evaluates to a scalar (non-array) value, the
`contextKey` SHALL be the context expression without an index suffix.

#### Scenario: Array context keys

- **WHEN** the context expression is `name` and the Patient has 3 name entries
- **THEN** the context keys SHALL be `name[0]`, `name[1]`, `name[2]`

#### Scenario: Complex context expression keys

- **WHEN** the context expression is `contact.telecom` and there are 2 telecom
  entries
- **THEN** the context keys SHALL be `contact.telecom[0]`,
  `contact.telecom[1]`

#### Scenario: Scalar context key

- **WHEN** the context expression evaluates to a single scalar value (not an
  array)
- **THEN** the context key SHALL be the context expression string without an
  index suffix

### Requirement: Per-element trace isolation

When evaluating with a context expression, trace output from `trace()` calls
in the main expression SHALL be scoped to the context element that produced
them. Each `ResultGroup` SHALL contain only the traces generated during
evaluation of its corresponding context element.

#### Scenario: Traces grouped per context element

- **WHEN** the expression `trace('trc').given.join(' ').combine(family).join(', ')`
  is evaluated with context `name` against a Patient with 3 name entries
- **THEN** each `ResultGroup` SHALL contain a trace with label `trc` holding
  only the `HumanName` value for that specific name entry

#### Scenario: No trace calls with context

- **WHEN** an expression without `trace()` is evaluated with a context
  expression
- **THEN** each `ResultGroup` SHALL have an empty `traces` list

### Requirement: Empty context handling

When the context expression evaluates to an empty collection (null or empty
array), the result SHALL contain zero `ResultGroup` entries.

#### Scenario: Empty context produces no result groups

- **WHEN** the context expression evaluates to an empty collection
- **THEN** the result SHALL contain an empty `resultGroups` list

### Requirement: Python result structure

The Python `evaluate_fhirpath` method SHALL return a dict with
`expectedReturnType` (string) and `resultGroups` (list of dicts). Each group
dict SHALL contain `contextKey` (string or None), `results` (list of typed
value dicts), and `traces` (list of trace dicts).

#### Scenario: Python non-context result

- **WHEN** `evaluate_fhirpath` is called without a context expression
- **THEN** the returned dict SHALL have a `resultGroups` list with one entry
  where `contextKey` is `None`

#### Scenario: Python context result

- **WHEN** `evaluate_fhirpath` is called with context expression `name`
- **THEN** the returned dict SHALL have a `resultGroups` list with one entry
  per context element, each with a string `contextKey`
