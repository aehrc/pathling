## Why

The FHIRPath `trace()` function is defined in the specification as a diagnostic
utility that logs intermediate collection values during expression evaluation.
Pathling does not currently support it, causing expressions that use `trace()`
to fail. Implementing it improves spec compliance and gives users a debugging
tool for complex FHIRPath expressions. See GitHub issue #2580.

## What Changes

- Add a custom Spark Catalyst expression (`TraceExpression`) that logs the
  string representation of each evaluated value via SLF4J, then returns the
  value unchanged.
- Add a new `UtilityFunctions` function provider class exposing `trace()` as a
  `@FhirPathFunction`, registered in the `StaticFunctionRegistry`.
- The initial implementation supports the `name` parameter only; the optional
  `projection` parameter is deferred to a follow-up.

## Capabilities

### New Capabilities

- `fhirpath-trace`: The FHIRPath `trace(name)` function that logs a string
  representation of the input collection under the given label, then returns
  the input unchanged.

### Modified Capabilities

(none)

## Impact

- `encoders` module: new Scala file `TraceExpression.scala` in
  `au.csiro.pathling.sql`.
- `fhirpath` module: new `UtilityFunctions.java` provider, registration in
  `StaticFunctionRegistry`.
- `fhirpath` module (test): new tests in `DatasetEvaluatorTest` or a dedicated
  test class verifying pass-through semantics and log output via
  `ListAppender`.
- Existing YAML reference tests for `trace()` should pass without exclusions.
