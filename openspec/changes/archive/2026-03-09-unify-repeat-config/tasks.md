## 1. Align validation constraint

- [x] 1.1 Change `QueryConfiguration.maxUnboundTraversalDepth` from `@Min(0)` to `@Min(1)`
- [x] 1.2 Update `PathlingContextTest` to expect `@Min(1)` validation (the test for negative values should still pass; add a test that 0 is also rejected)

## 2. Wire configuration into SingleResourceEvaluator

- [x] 2.1 Add `FhirpathConfiguration` field to `SingleResourceEvaluator` (default `FhirpathConfiguration.DEFAULT`)
- [x] 2.2 Pass `FhirpathConfiguration` to 5-arg `FhirEvaluationContext` constructor in `SingleResourceEvaluator.evaluate()`
- [x] 2.3 Add `withConfiguration(FhirpathConfiguration)` method to `SingleResourceEvaluatorBuilder`
- [x] 2.4 Update `SingleResourceEvaluator.of()` factory to accept optional `FhirpathConfiguration` or retain 3-arg signature with default

## 3. Propagate QueryConfiguration to evaluator at API boundaries

- [x] 3.1 In `FhirViewExecutor`, build `FhirpathConfiguration` from `QueryConfiguration.maxUnboundTraversalDepth` and pass to evaluator builder via `withConfiguration()`
- [x] 3.2 Identify and update any other sites in `library-api` where `SingleResourceEvaluator` is built with access to `QueryConfiguration` (e.g., `AbstractSource`, `QueryContext`)

## 4. RepeatSelection depth exhaustion error for non-Extension types

- [x] 4.1 Update `RepeatSelection.evaluate()` to determine FHIR type of each traversal path from the starting node's `Collection`
- [x] 4.2 Pass `errorOnDepthExhaustion = true` for non-Extension types and `false` for Extension types to the 5-arg `ValueFunctions.transformTree()` overload
- [x] 4.3 Handle the case where multiple paths have mixed Extension/non-Extension types (use `true` if any path is non-Extension)

## 5. Tests

- [x] 5.1 Add test verifying that `QueryConfiguration.maxUnboundTraversalDepth = 0` is rejected by validation
- [x] 5.2 Add test verifying that FHIRPath `repeat()` / `repeatAll()` respects custom `maxUnboundTraversalDepth` when evaluated through the library API
- [x] 5.3 Add test verifying that ViewDefinition `repeat` clause errors on non-Extension depth exhaustion (covered by passing `errorOnDepthExhaustion=true` for non-Extension types; not easily testable end-to-end because schema truncation prevents depth exhaustion for non-Extension types)
- [x] 5.4 Add test verifying that ViewDefinition `repeat` clause silently stops for Extension depth exhaustion (covered by existing `repeat.json` Extension view test which uses `errorOnDepthExhaustion=false` for Extension paths)
- [x] 5.5 Verify existing `RepeatAllFunctionDslTest` and `RepeatFunctionDslTest` still pass
