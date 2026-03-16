## Why

The `repeatAll()` function uses a hardcoded same-type recursion depth limit of 10. This value needs to be configurable, and its semantics need refinement:
same-type depth exhaustion currently silently stops traversal, but for
non-Extension types this indicates infinite recursion (e.g.,
`repeatAll($this)`, `repeatAll(first())`) and should produce an evaluation
error. Only Extension traversal is a legitimate same-type recursion that should
silently stop at the depth limit.

## What Changes

- Introduce a `FhirpathConfiguration` object that holds configurable FHIRPath
  evaluation parameters, starting with `maxExtensionDepth` (renamed from
  `DEFAULT_SAME_TYPE_RECURSION_DEPTH`).
- Pass `FhirpathConfiguration` through the `EvaluationContext` so it is
  accessible to FHIRPath functions at evaluation time.
- Add `EvaluationContext` as a supported parameter type in
  `FunctionParameterResolver`, allowing `@FhirPathFunction`-annotated methods to
  receive the evaluation context.
- Add an `errorOnDepthExhaustion` flag to `UnresolvedTransformTree` (Scala
  Catalyst expression) so that same-type depth exhaustion can either silently
  stop (for Extension traversal) or raise an error (for all other same-type
  traversal).
- `repeatAll()` detects whether the projection targets the Extension FHIR type
  and selects the appropriate mode: silent stop for Extension, error for
  everything else.

## Capabilities

### New Capabilities

- `fhirpath-configuration`: A configuration object for FHIRPath evaluation
  parameters, initially containing `maxExtensionDepth`, passed through the
  evaluation context.

### Modified Capabilities

- `fhirpath-repeat-all`: Same-type depth exhaustion now raises an error for
  non-Extension traversals (indicating infinite recursion), while Extension
  traversal continues to silently stop at the configured `maxExtensionDepth`.

## Impact

- **fhirpath module**: New `FhirpathConfiguration` class, `EvaluationContext`
  interface gains `getConfiguration()`, `FunctionParameterResolver` supports
  `EvaluationContext` injection, `FilteringAndProjectionFunctions.repeatAll()`
  gains `EvaluationContext` parameter.
- **encoders module**: `UnresolvedTransformTree` gains
  `errorOnDepthExhaustion` flag, `ValueFunctions.transformTree()` and
  `variantTransformTree()` gain corresponding parameter.
- **library-api**: `FhirEvaluationContext` updated to carry
  `FhirpathConfiguration`.
- **Testing**: Existing `repeatAll(first())` test updated to expect an error
  instead of bounded results. New tests for infinite recursion detection and
  configurable depth.
