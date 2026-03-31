### Requirement: trace returns input collection unchanged

The `trace(name)` function SHALL return the input collection without
modification. The result collection SHALL have the same type, cardinality, and
values as the input collection.

#### Scenario: trace on a singleton string

- **WHEN** evaluating `Patient.name.family.trace('debug')`
- **THEN** the result SHALL be identical to evaluating `Patient.name.family`

#### Scenario: trace on a boolean value

- **WHEN** evaluating `Patient.active.trace('active-check')`
- **THEN** the result SHALL be identical to evaluating `Patient.active`

#### Scenario: trace on an empty collection

- **WHEN** evaluating `{}.trace('empty')`
- **THEN** the result SHALL be an empty collection

#### Scenario: trace on a collection with null values

- **WHEN** evaluating a path that produces null values followed by `.trace('label')`
- **THEN** null values SHALL be preserved in the output

#### Scenario: trace on a complex type

- **WHEN** evaluating `Patient.name.trace('names')` where name is a HumanName
  complex type
- **THEN** the result SHALL be identical to evaluating `Patient.name`

#### Scenario: trace after a where filter

- **WHEN** evaluating `Patient.name.where(use = 'official').trace('official-names')`
- **THEN** the result SHALL be identical to evaluating
  `Patient.name.where(use = 'official')`

#### Scenario: trace before a where filter

- **WHEN** evaluating `Patient.name.trace('all-names').where(use = 'official')`
- **THEN** the result SHALL be identical to evaluating
  `Patient.name.where(use = 'official')`

#### Scenario: two trace calls in a single expression

- **WHEN** evaluating
  `Patient.name.trace('before-filter').where(use = 'official').trace('after-filter')`
- **THEN** the result SHALL be identical to evaluating
  `Patient.name.where(use = 'official')`

#### Scenario: trace on a multi-element collection

- **WHEN** evaluating `Patient.name.given.trace('givens')` where the Patient
  has multiple given names
- **THEN** the result SHALL contain all given names unchanged

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

### Requirement: trace is a recognised FHIRPath function

The `trace` function SHALL be registered in the `StaticFunctionRegistry` and
recognised by the FHIRPath parser. Expressions containing `trace()` SHALL NOT
produce parse errors or "unknown function" errors.

#### Scenario: trace function is callable

- **WHEN** parsing and evaluating the expression `Patient.active.trace('test')`
- **THEN** no error SHALL be raised

#### Scenario: trace with missing name argument is an error

- **WHEN** parsing the expression `Patient.active.trace()`
- **THEN** an error SHALL be raised indicating a missing required argument
