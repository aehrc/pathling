## Why

The `repeatAll()` FHIRPath function and the ViewDefinition `repeat` clause both
perform recursive traversal with depth limiting, but they use separate
configuration classes (`FhirpathConfiguration` vs `QueryConfiguration`) with
inconsistent validation constraints, and the library API does not wire
`QueryConfiguration.maxUnboundTraversalDepth` through to FHIRPath evaluation.
Additionally, the `repeat` clause lacks runtime error detection for infinite
recursion on non-Extension types, silently truncating instead.

## What Changes

- Align the `@Min` validation constraint on
  `QueryConfiguration.maxUnboundTraversalDepth` from `@Min(0)` to `@Min(1)`,
  matching `FhirpathConfiguration`.
- Wire `QueryConfiguration.maxUnboundTraversalDepth` into the FHIRPath evaluator
  so that `repeat()` and `repeatAll()` functions respect the user-configured
  depth when invoked through the library API.
- Add `errorOnDepthExhaustion` parameter to `ValueFunctions.transformTree()` so
  the ViewDefinition `repeat` clause can error at runtime when non-Extension
  traversals exhaust the depth limit.
- Update `RepeatSelection` to determine the FHIR type of each traversal path
  and pass the appropriate error-on-exhaustion flag.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `fhirpath-configuration`: `QueryConfiguration` constraint alignment and
  wiring `maxUnboundTraversalDepth` into FHIRPath evaluation context.

## Impact

- `fhirpath/src/main/java/au/csiro/pathling/config/QueryConfiguration.java` —
  validation constraint change (`@Min(0)` to `@Min(1)`). **BREAKING** for
  callers that set `maxUnboundTraversalDepth` to 0.
- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/evaluation/SingleResourceEvaluator.java` —
  accept and propagate `FhirpathConfiguration`.
- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/evaluation/SingleResourceEvaluatorBuilder.java` —
  builder support for `FhirpathConfiguration`.
- `encoders/src/main/java/au/csiro/pathling/encoders/ValueFunctions.java` —
  add `errorOnDepthExhaustion` parameter to `transformTree()`.
- `fhirpath/src/main/java/au/csiro/pathling/projection/RepeatSelection.java` —
  determine FHIR type per path, pass error flag.
- `fhirpath/src/main/java/au/csiro/pathling/views/FhirViewExecutor.java` —
  propagate configuration to evaluator.
- `library-api/` — map `QueryConfiguration.maxUnboundTraversalDepth` to
  `FhirpathConfiguration` at the API boundary.
- Test updates across `PathlingContextTest`, `RepeatSelection` tests, and
  existing DSL tests.
