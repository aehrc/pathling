## Context

The `flattenExtensions` method in `SerializerBuilderProcessor` recursively
collects extensions from all nested elements within a FHIR resource and stores
them in a resource-level `_extension` map (keyed by `System.identityHashCode`).
At query time, the FHIRPath engine uses the `_fid` field on each composite
element to look up its extensions from this map.

The traversal uses `Base.children()` to discover child properties, then
`Base.getProperty(hash, name, false)` to retrieve their values. For choice
types (e.g., `value[x]`), `children()` returns the property name with the
`[x]` suffix, but `getProperty()` uses a switch on the hash of the base name
(without `[x]`). This hash mismatch causes `getProperty()` to return null,
silently skipping the entire subtree.

## Goals / non-goals

**Goals:**

- Restore correct extension collection through choice type elements.
- Add tests that reproduce the bug before fixing it (TDD approach).
- Verify the fix works for both encoding (extension storage) and FHIRPath
  querying (extension retrieval).

**Non-goals:**

- Changing the extension storage architecture (resource-level `_extension`
  map).
- Adding support for extensions on primitive types (a separate concern).
- Modifying any public API.

## Decisions

### Use `Property.getValues()` instead of `Base.getProperty()`

**Choice**: Replace the `children()` → `getProperty()` traversal with direct
use of `Property.getValues()`.

**Rationale**: The `Property` object returned by `children()` already contains
the values (populated by `listChildren()`). Using `getValues()` directly
bypasses the hash-based lookup entirely, eliminating the mismatch for choice
types and all other property types.

**Alternative considered**: Stripping the `[x]` suffix before computing the
hash. Rejected because it's fragile (depends on HAPI naming conventions) and
`getValues()` is simpler and more direct.

### Test with Observation.value[x] as the primary case

**Choice**: Use an Observation with `valueCodeableConcept` containing a Coding
with an extension, mirroring the original bug report.

**Rationale**: This directly reproduces the reported issue. Observation is
already well-covered in the test suite, and `value[x]` is one of the most
commonly used choice types.

### Test at both encoder and FHIRPath levels

**Choice**: Add tests in both the encoders module (verifying `_extension` map
contents) and the FHIRPath module (verifying `extension()` function works on
Coding).

**Rationale**: The bug manifests at the encoding level (extensions not
collected) but is observed by users at the FHIRPath level (queries return
null). Testing both layers ensures comprehensive coverage.

## Risks / trade-offs

- **Risk**: `Property.getValues()` may behave differently for unset properties
  (returning empty list vs null). **Mitigation**: The current code already
  handles nulls via `.filter(_ != null)` and the `Property` constructor handles
  null values by not adding them, so empty lists are safe.
- **Risk**: Changing traversal may affect performance for deeply nested
  resources. **Mitigation**: `getValues()` is a simple getter returning a
  pre-populated list; it should be equivalent or faster than the hash-based
  lookup.
