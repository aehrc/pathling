## MODIFIED Requirements

### Requirement: trace logs values via SLF4J

The `trace(name)` function SHALL log a string representation of each evaluated
value using an SLF4J logger. The log message SHALL include the `name` argument
as a label to identify the trace point.

When a `TraceCollector` is available on the `EvaluationContext`, the function
SHALL additionally add each traced value to the collector with the trace label
and the FHIR type of the input collection.

#### Scenario: trace produces log output with label

- **WHEN** evaluating `Patient.active.trace('myLabel')` and materialising the
  result
- **THEN** the SLF4J logger for `TraceExpression` SHALL emit at least one log
  entry containing the string `myLabel`

#### Scenario: trace logs the value representation

- **WHEN** evaluating `Patient.name.family.trace('names')` against a Patient
  with family name `Smith` and materialising the result
- **THEN** the log output SHALL contain a representation of `Smith`

#### Scenario: two trace calls produce distinct log entries

- **WHEN** evaluating
  `Patient.name.trace('before').where(use = 'official').trace('after')` and
  materialising the result
- **THEN** the log output SHALL contain entries labelled `before` and entries
  labelled `after`

#### Scenario: trace populates collector when present

- **WHEN** evaluating `Patient.name.trace('names')` with a `TraceCollector`
  attached to the evaluation context and materialising the result
- **THEN** the collector SHALL contain entries under label `names` with FHIR
  type `HumanName`
