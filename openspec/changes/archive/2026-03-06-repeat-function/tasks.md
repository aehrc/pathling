## 1. Core Implementation

- [x] 1.1 Add `allowPrimitiveSelfRef` boolean parameter to `Collection.repeatAll()` — when `true`, return level_0 instead of throwing error for self-referential primitives
- [x] 1.2 Update `FilteringAndProjectionFunctions.repeatAll()` to pass `false` for the new parameter (preserving existing behavior)
- [x] 1.3 Add `repeat()` method to `Collection` that calls `repeatAll(transform, maxDepth, true)` then deduplicates using the collection's comparator for Equatable types
- [x] 1.4 Add `repeat` function to `FilteringAndProjectionFunctions` with `@FhirPathFunction` annotation

## 2. Testing

- [x] 2.1 Verify YAML reference tests pass (`5.2.3_repeat.yaml`, repeat cases in `5.2_filtering_and_projection.yaml`, repeat cases in `fhir-r4.yaml`)
- [x] 2.2 Add exclusions for any YAML tests that require unsupported features (e.g., primitive element `id` attribute)
- [x] 2.3 Write DSL tests for `repeat()` covering: basic tree traversal, primitive dedup, quantity dedup, complex type no-dedup, empty input, chained operations
