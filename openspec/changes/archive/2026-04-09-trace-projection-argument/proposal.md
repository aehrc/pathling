## Why

The FHIRPath `trace()` function supports an optional second argument
(`projection: Expression`) that allows logging a derived value instead of the
full input collection. This is part of the FHIRPath specification but was
explicitly out of scope in #2580. Adding it completes the trace implementation.

## What Changes

- Modify `TraceExpression` from a unary to a binary Spark Catalyst expression
  that returns the first child (input) and logs the second child (projected
  value). When no projection is provided, both children are the same column.
- Mark `TraceExpression` as `Nondeterministic` to prevent Spark from optimizing
  away its logging side effects.
- Add an optional `CollectionTransform` parameter to the `trace()` function in
  `UtilityFunctions` for the projection expression.
- When a projection is provided, the logged FHIR type is the projected
  collection's type, not the input's type.

## Capabilities

### New Capabilities

### Modified Capabilities

- `fhirpath-trace`: Add support for the optional projection argument. The trace
  function accepts a second expression argument that is evaluated against the
  input and logged, while the input is still returned unchanged.
- `fhirpath-trace-collector`: The FHIR type captured in trace entries reflects
  the type of the expression being logged (projected type when projection is
  used).

## Impact

- `encoders` module: `TraceExpression.scala` changes from unary to binary
  expression, marked nondeterministic.
- `fhirpath` module: `UtilityFunctions.trace()` gains an optional
  `CollectionTransform` parameter.
- `fhirpath` module: New tests for projection behavior.
- No changes to `library-api`, Python, or R bindings (projection is a FHIRPath
  syntax feature, transparent to callers).
