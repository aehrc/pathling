## Why

The FHIRPath specification includes a `repeatAll()` STU function for recursive
traversal of hierarchical FHIR structures (e.g., `Questionnaire.item`). Unlike
`repeat()`, it skips equality-based deduplication, making it feasible to
implement in Spark's columnar model where element-wise equality on complex
nested
FHIR types is impractical. Supporting `repeatAll()` enables users to flatten
deeply nested FHIR data without manually chaining navigation steps or resorting
to `descendants()`.

## What Changes

- Add `repeatAll(projection)` as a new FHIRPath function available in Pathling's
  FHIRPath engine, conforming to the STU specification.
- The function accepts a scoped projection expression (with `$this` binding) and
  recursively applies it, collecting all results including duplicates, until no
  new items are produced.
- Traversal always navigates to the deepest level possible regardless of the
  encoding's maximum nesting level. Recursion depth is only limited when the
  traversal expression is self-referential (i.e. navigates to a node of the same
  type), in which case a hardcoded depth limit (of 10) prevents infinite loops —
  matching
  the approach used by the SQL on FHIR `repeat` clause implementation.
- The function is available wherever FHIRPath expressions are evaluated today,
  including the library API and Python bindings, with no interface changes.

## Capabilities

### New Capabilities

- `fhirpath-repeat-all`: The FHIRPath `repeatAll()` STU function for recursive
  collection traversal without deduplication.

### Modified Capabilities

- `fhirpath-evaluation-api`: The evaluation API gains support for `repeatAll()`
  expressions, requiring no interface changes but expanding the set of
  recognised FHIRPath functions.

## Impact

- **fhirpath module**: New function registration, new method on `Collection`.
- **encoders module**: May require new helper utilities to support recursive
  traversal with schema compatibility across nesting levels.
- **library-api / Python bindings**: No API changes — `repeatAll()` becomes
  available automatically through the existing expression evaluation pipeline.
- **Testing**: New unit tests covering flat traversal, multi-level recursive
  structures (e.g., `Questionnaire.item`), empty input, and expressions chained
  after `repeatAll()`. Tests should verify two cross-cutting invariants that
  apply to all element-propagating FHIRPath functions but are especially
  important to validate for `repeatAll()` given the Variant round-trip and
  array-oriented tree traversal involved:
    - **Extension preservation**: Extensions and other sub-elements are preserved
      in propagated results.
    - **Singular/collection handling**: The function works correctly with
      projections that produce singular values (e.g., `Patient.repeatAll(gender)`)
      or primitive types, not only collection-valued projections that return
      arrays.
