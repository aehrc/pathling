## Why

The FHIRPath `repeat()` function is a standard function for recursive tree traversal with deduplication. It is needed for common FHIR patterns like `Questionnaire.repeat(item)` and `ValueSet.expansion.repeat(contains)`. Pathling already implements `repeatAll()` (the STU variant without dedup), so `repeat()` can be built on top of it with minimal effort.

## What Changes

- Add the FHIRPath `repeat(projection: expression)` function, implemented as `repeatAll()` followed by equality-based deduplication.
- For collections implementing `Equatable` (FHIRPath primitive types, Coding, Quantity, etc.), deduplication uses the collection's comparator — either `array_distinct` for default SQL equality or `arrayDistinctWithEquality` for custom equality (quantities with unit conversion, etc.).
- For complex types (BackboneElement, etc.), no deduplication is performed — structural synthetic fields like `_fid` make struct equality impractical, and tree-structured FHIR data doesn't produce duplicates in practice.

## Capabilities

### New Capabilities

- `fhirpath-repeat`: The FHIRPath `repeat()` function for recursive tree traversal with equality-based deduplication.

### Modified Capabilities

## Impact

- `fhirpath` module: New `repeat()` method on `Collection`, new `@FhirPathFunction` in `FilteringAndProjectionFunctions`.
- Enables existing YAML reference tests (`5.2.3_repeat.yaml`, `5.2_filtering_and_projection.yaml` repeat cases, `fhir-r4.yaml` repeat cases).
- No breaking changes — additive only.
