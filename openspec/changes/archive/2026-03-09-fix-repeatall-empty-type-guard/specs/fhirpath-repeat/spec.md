## ADDED Requirements

### Requirement: repeat function rejects indeterminate types via repeatAll

The `repeat` function SHALL inherit the indeterminate type guard from
`repeatAll`. When the traversal expression produces a result with indeterminate
FHIR type (e.g., an unresolved choice element), `repeat` SHALL raise the same
error as `repeatAll`.

#### Scenario: Choice element identity traversal raises indeterminate type error

- **WHEN** `value.repeat($this)` is evaluated on an Observation resource where
  `value` is a choice type (value[x])
- **THEN** the function SHALL raise an error indicating indeterminate FHIR type

#### Scenario: Choice element with ofType resolves and terminates

- **WHEN** `repeat(value.ofType(Quantity))` is evaluated on an Observation
  resource
- **THEN** the function SHALL return the Quantity value from level_0 directly,
  since applying `value.ofType(Quantity)` to a Quantity produces an empty
  level_1 (Quantity.value is a decimal, ofType(Quantity) on decimal is empty)

### Requirement: repeat function handles self-referential complex expressions

The `repeat` function SHALL correctly handle expressions that produce same-type
results at every depth level. Because `repeat()` delegates to `repeatAll()`
with self-reference allowed, depth exhaustion does not raise an error. The
depth-limited results are deduplicated by `repeat()`, which guarantees
termination and produces the correct result.

#### Scenario: Resource-level identity traversal returns input

- **WHEN** `repeat($this)` is evaluated on a resource (e.g., Patient)
- **THEN** the function SHALL return the resource unchanged, since the first
  iteration produces the same item which is already in the result set,
  causing immediate termination via deduplication

#### Scenario: Resource-level %resource projection returns deduplicated results

- **WHEN** `name.repeat(%resource).gender` is evaluated on a Patient resource
- **THEN** the function SHALL return the patient's gender value, since
  `%resource` produces the same Patient at each depth level and deduplication
  collapses them to one
