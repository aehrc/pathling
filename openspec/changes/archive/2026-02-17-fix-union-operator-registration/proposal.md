## Why

The FHIRPath union operator (`|`) is fully implemented in `UnionOperator` but
never registered in the operator resolution chain. Parsing `|` throws
`UnsupportedFhirPathFeatureError`, and the DSL tests that cover it are silently
skipped by the test framework (which catches that error and converts it to a
JUnit `TestAbortedException`).

## What changes

- Register the `UnionOperator` in `BinaryOperatorType` so that parsing `|`
  resolves to the existing implementation.
- Ensure the DSL tests in `CombiningOperatorsDslTest` actually execute rather
  than being silently skipped.

## Capabilities

### New capabilities

(none)

### Modified capabilities

(none - this is a bug fix, not a requirement change)

## Impact

- `BinaryOperatorType.java` — add a new enum constant.
- `CombiningOperatorsDslTest` — tests will transition from skipped to passing.
- All contexts that parse FHIRPath (server, library API, fhirpath-lab-api) gain
  working union operator support.
