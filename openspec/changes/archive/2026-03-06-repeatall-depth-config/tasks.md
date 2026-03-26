## 1. UnresolvedTransformTree error mode

- [x] 1.1 Add `errorOnDepthExhaustion: Boolean = false` parameter to `UnresolvedTransformTree` case class in `Expressions.scala`, propagating it through recursive construction in `mapChildren` and `withNewChildrenInternal`
- [x] 1.2 When `errorOnDepthExhaustion` is `true` and same-type depth is exhausted (`level <= 0 && parentType.contains(newValue.dataType)`), throw an `AnalysisException` with message "Infinite recursive traversal detected." instead of returning an empty array
- [x] 1.3 Add `errorOnDepthExhaustion` boolean parameter to `ValueFunctions.transformTree()` and forward it to `UnresolvedTransformTree`
- [x] 1.4 Add `errorOnDepthExhaustion` boolean parameter to `ValueFunctions.variantTransformTree()` and forward it to `transformTree()`
- [x] 1.5 Update `RepeatSelection` to pass `false` for `errorOnDepthExhaustion` to `ValueFunctions.transformTree()`, preserving current silent-stop behavior
- [x] 1.6 Write unit tests for `UnresolvedTransformTree` verifying: (a) silent stop when `errorOnDepthExhaustion = false`, (b) error thrown when `errorOnDepthExhaustion = true` and same-type depth exhausted

## 2. FhirpathConfiguration

- [x] 2.1 Create `FhirpathConfiguration` record in the `fhirpath` module with `maxExtensionDepth` field (int), `DEFAULT_MAX_EXTENSION_DEPTH` constant (10), and `DEFAULT` static instance
- [x] 2.2 Add `getConfiguration()` default method to `EvaluationContext` interface returning `FhirpathConfiguration.DEFAULT`
- [x] 2.3 Add `FhirpathConfiguration` field to `FhirEvaluationContext` record and override `getConfiguration()`
- [x] 2.4 Update all construction sites of `FhirEvaluationContext` to pass `FhirpathConfiguration.DEFAULT` (or a configured value where applicable)

## 3. EvaluationContext parameter injection

- [x] 3.1 Add support for `EvaluationContext` as a parameter type in `FunctionParameterResolver.resolveArgument()` — inject the evaluation context without consuming a FHIRPath argument
- [x] 3.2 Update the argument count validation in `FunctionParameterResolver.bind()` to exclude context-injected parameters from the expected argument count
- [x] 3.3 Write unit tests for `FunctionParameterResolver` verifying that `EvaluationContext` parameters are injected correctly alongside normal Collection and CollectionTransform parameters

## 4. repeatAll() integration

- [x] 4.1 Update `FilteringAndProjectionFunctions.repeatAll()` to accept an `EvaluationContext` parameter and read `maxExtensionDepth` from the configuration
- [x] 4.2 Remove the `DEFAULT_SAME_TYPE_RECURSION_DEPTH` constant from `FilteringAndProjectionFunctions`
- [x] 4.3 Update `Collection.repeatAll()` to accept an `errorOnDepthExhaustion` boolean parameter and pass it through to `ValueFunctions.variantTransformTree()`
- [x] 4.4 Add Extension detection in `FilteringAndProjectionFunctions.repeatAll()` or `Collection.repeatAll()`: check `resultTemplate.getFhirType()` for `FHIRDefinedType.EXTENSION` and set `errorOnDepthExhaustion = !isExtensionTraversal`

## 5. Tests

- [x] 5.1 Update the existing `repeatAll(first())` DSL test to expect an evaluation error instead of bounded results (count of 11)
- [x] 5.2 Add DSL test for `repeatAll($this)` expecting an evaluation error
- [x] 5.3 Verify existing `repeatAll(extension)` test still passes with silent-stop behavior
- [ ] 5.4 ~~Add DSL test with a custom `FhirpathConfiguration` setting `maxExtensionDepth` to a lower value and verify Extension traversal respects it~~ — Deferred: requires adding configuration support to DSL test infrastructure
- [x] 5.5 Verify existing `repeatAll(item)` tests still pass (schema-truncated traversal unaffected)
