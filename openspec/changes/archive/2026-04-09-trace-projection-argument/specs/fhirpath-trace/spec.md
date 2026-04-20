## ADDED Requirements

### Requirement: trace with projection logs projected value and returns input unchanged

The `trace(name, projection)` function SHALL evaluate the projection expression
against the input collection and log the projected result. The function SHALL
return the input collection unchanged, regardless of the projection result.

#### Scenario: trace with projection on a primitive path

- **WHEN** evaluating `Patient.name.trace('ids', id)` where `id` is a valid
  path on HumanName
- **THEN** the result SHALL be identical to evaluating `Patient.name`
- **AND** the logged value SHALL be the result of evaluating `id` on each
  HumanName element

#### Scenario: trace with projection on a complex expression

- **WHEN** evaluating `Patient.name.trace('full', given.first() + ' ' + family)`
- **THEN** the result SHALL be identical to evaluating `Patient.name`
- **AND** the logged value SHALL be the concatenated string

#### Scenario: trace with projection returning empty

- **WHEN** evaluating `Patient.name.trace('missing', deceased)` where `deceased`
  does not exist on HumanName
- **THEN** the result SHALL be identical to evaluating `Patient.name`
- **AND** no value SHALL be logged for that element

#### Scenario: trace with projection on an empty input collection

- **WHEN** evaluating `{}.trace('empty', id)`
- **THEN** the result SHALL be an empty collection

### Requirement: trace is a nondeterministic expression

The Spark Catalyst expression underlying `trace()` SHALL be marked as
nondeterministic to prevent the query optimizer from eliminating trace
expressions or caching their results via common subexpression elimination. Each
`trace()` call SHALL execute its logging side effect independently.

#### Scenario: duplicate trace calls both execute

- **WHEN** evaluating an expression where the same `trace()` call appears in
  two branches of a computation
- **THEN** both trace calls SHALL produce log output independently

## MODIFIED Requirements

### Requirement: trace returns input collection unchanged

The `trace(name [, projection])` function SHALL return the input collection
without modification. The result collection SHALL have the same type,
cardinality, and values as the input collection. This applies regardless of
whether a projection argument is provided.

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

#### Scenario: trace with projection returns input not projection

- **WHEN** evaluating `Patient.name.trace('label', family)`
- **THEN** the result SHALL be identical to evaluating `Patient.name`
- **AND** the result SHALL NOT be the family names

### Requirement: trace logs values via SLF4J

The `trace(name [, projection])` function SHALL log a string representation of
each evaluated value using an SLF4J logger. When a projection is provided, the
logged value SHALL be the result of evaluating the projection on the input, not
the input itself. The log message SHALL include the `name` argument as a label.

When a `TraceCollector` is available on the `EvaluationContext`, the function
SHALL additionally add each traced value to the collector with the trace label
and the FHIR type of the logged expression.

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

#### Scenario: trace with projection logs the projected value

- **WHEN** evaluating `Patient.name.trace('fam', family)` against a Patient
  with family name `Smith` and materialising the result
- **THEN** the log output SHALL contain a representation of `Smith`
- **AND** the log output SHALL NOT contain a full HumanName representation

#### Scenario: trace populates collector when present

- **WHEN** evaluating `Patient.name.trace('names')` with a `TraceCollector`
  attached to the evaluation context and materialising the result
- **THEN** the collector SHALL contain entries under label `names` with FHIR
  type `HumanName`

#### Scenario: trace with projection populates collector with projected type

- **WHEN** evaluating `Patient.name.trace('fam', family)` with a
  `TraceCollector` attached to the evaluation context and materialising the
  result
- **THEN** the collector SHALL contain entries under label `fam` with FHIR type
  `string` (the type of the projected expression, not `HumanName`)

### Requirement: trace is a recognised FHIRPath function

The `trace` function SHALL be registered in the `StaticFunctionRegistry` and
recognised by the FHIRPath parser. Expressions containing `trace()` with one or
two arguments SHALL NOT produce parse errors or "unknown function" errors.

#### Scenario: trace function is callable

- **WHEN** parsing and evaluating the expression `Patient.active.trace('test')`
- **THEN** no error SHALL be raised

#### Scenario: trace with projection is callable

- **WHEN** parsing and evaluating `Patient.name.trace('label', family)`
- **THEN** no error SHALL be raised

#### Scenario: trace with missing name argument is an error

- **WHEN** parsing the expression `Patient.active.trace()`
- **THEN** an error SHALL be raised indicating a missing required argument
