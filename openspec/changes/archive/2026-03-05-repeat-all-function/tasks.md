## 1. Spike: Variant round-trip validation

- [x] 1.1 Write a spike test that encodes a Questionnaire with nested `item` elements into a Spark DataFrame, converts items to Variant via `to_variant_object()`, and converts back via `variant_get()` with the level-0 target schema — verify all FHIR data types round-trip faithfully
- [x] 1.2 Validate that `variant_get` fills missing fields with null when converting from a shallower struct schema to the full target schema

## 2. Variant wrapping utility

- [x] 2.1 Add a helper method in `ValueFunctions` (or a new utility class in `encoders`) that wraps a column transform with the Variant round-trip: apply transform → `to_variant_object()` each element → return `Array[Variant]`
- [x] 2.2 Add a helper method that converts the final `Array[Variant]` result back to `Array[targetSchema]` via `variant_get(v, '$', targetSchema)`
- [x] 2.3 Write unit tests for the Variant wrapping helpers covering struct arrays with differing schemas, empty arrays, and null elements

## 3. Collection.repeatAll method

- [x] 3.1 Add `repeatAll(CollectionTransform, int)` method on `Collection` that pre-computes level-0 results via the column transform, then passes them to `ValueFunctions.transformTree()` with a vectorised `to_variant_object` extractor and the column transform as the traversal, and unwraps the result via `variantUnwrap` using the level-0 expression as the schema reference
- [x] 3.2 Determine the target schema from the resolved column type at level 0 before applying the Variant wrapping
- [x] 3.3 Write unit tests for `Collection.repeatAll` covering single-level traversal, multi-level recursive traversal, empty input, and chained expressions

## 4. FHIRPath function registration

- [x] 4.1 Add a `@FhirPathFunction`-annotated static `repeatAll` method in `FilteringAndProjectionFunctions` accepting `Collection` and `CollectionTransform`, delegating to `Collection.repeatAll` with a hardcoded same-type depth limit of 10
- [x] 4.2 Register the function provider class in `StaticFunctionRegistry` (if not already included)
- [x] 4.3 Write FHIRPath-level integration tests: `Questionnaire.repeatAll(item)`, `Questionnaire.repeatAll(item).linkId`, `Questionnaire.repeatAll(item).where(type = 'group')`, `Questionnaire.repeatAll(item).count()`, empty input, and verify that extensions and sub-elements are preserved in propagated results

## 5. Evaluation API integration test

- [x] 5.1 Add an integration test that evaluates `repeatAll(item).linkId` against a Questionnaire JSON resource via the library API evaluation method, verifying linkId values from all nesting levels are returned

## 6. Singular projection support

- [x] 6.1 Add `plural()` normalization to the level-0 result and inner traversal result in `Collection.repeatAll()` so that singular projections are wrapped into arrays before entering `variantTransformTree`
- [x] 6.2 Write DSL tests for singular projections: `Patient.repeatAll(gender)` (singular primitive) and `Patient.repeatAll(maritalStatus)` (singular complex type), verifying they return the same result as `select()`
