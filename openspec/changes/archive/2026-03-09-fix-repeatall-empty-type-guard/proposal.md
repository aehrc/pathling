## Why

The `repeatAll()` type consistency check silently passes when both level_0 and
level_1 have indeterminate FHIR types (`Optional.empty()`). This is reachable
through real user expressions involving FHIR choice types — for example,
`Observation.value.repeat($this)` preserves the MixedCollection with empty
fhirType through the identity transform, bypasses the type guard, and falls into
depth-limited recursion with an `UnsupportedRepresentation`, causing undefined
behavior.

## What Changes

- Add a guard in `repeatAll()` that rejects traversal when either level_0 or
  level_1 has an indeterminate FHIR type, before the existing type equality
  comparison.
- Add test coverage for:
    - Choice type expressions that hit the new indeterminate type guard
      (e.g., `value.repeat($this)`, `value.repeatAll($this)`).
    - Choice type expressions that fail at earlier points with existing error
      messages (e.g., `repeatAll(value)`, `value.repeatAll(first())`).
    - Choice type expressions that terminate normally
      (e.g., `repeat(value.ofType(Quantity))`).
    - Resource-level degenerate expressions that hit depth exhaustion
      (e.g., `repeat($this)`, `name.repeatAll(%resource).gender`).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `fhirpath-repeat-all`: Add a new classification case to the static type
  analysis gate — when either level_0 or level_1 has an indeterminate FHIR type,
  the function must raise an error. Add scenarios for choice type and
  resource-level degenerate expressions.
- `fhirpath-repeat`: Add scenarios for choice type and resource-level degenerate
  expressions (inherits the new guard behavior from `repeatAll`).

## Impact

- `Collection.java` — `repeatAll()` method in the fhirpath module.
- `RepeatAllFunctionDslTest.java` — new test cases.
- `RepeatFunctionDslTest.java` — new test cases.
- No API changes, no breaking changes. This converts undefined behavior into a
  clear error message.
