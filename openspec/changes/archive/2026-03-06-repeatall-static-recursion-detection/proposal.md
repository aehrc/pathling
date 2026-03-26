## Why

The current `repeatAll()` implementation uses a primitive type shortcut that
bypasses infinite recursion detection. Expressions like `gender.repeatAll($this)`
or `gender.repeatAll('value')` silently return a truncated result instead of
raising an error. The recursion detection logic should be moved earlier — to a
static analysis phase in `Collection.repeatAll()` — where self-referential
primitive traversals can be caught immediately, and non-recursive traversals
(both primitive and complex) can be shortcut to equivalent `select()` behavior
without entering the tree traversal at all.

## What Changes

- Add static type analysis in `Collection.repeatAll()` that applies the
  traversal expression twice (level_0 and level_1) to determine recursion
  behavior before any tree traversal.
- If level_1 is empty: return level_0 directly (equivalent to `select()`) for
  both primitive and complex types.
- If level_1 is same FHIR type as level_0 and primitive: raise an immediate
  error with a distinct message for self-referential primitive traversal.
- If level_1 is same FHIR type as level_0 and complex: proceed to
  `variantTransformTree()` (with `errorOnDepthExhaustion=false` for Extension,
  `true` for all others).
- If level_1 is a different non-empty FHIR type from level_0: raise an error
  for inconsistent traversal types.
- Remove the `isPrimitive` shortcut that currently bypasses recursion detection.
- Use distinct error messages to differentiate static detection (primitive
  self-reference, inconsistent types) from analysis-time detection (complex
  same-SQL-type depth exhaustion).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `fhirpath-repeat-all`: Recursion detection moves from purely analysis-time to
  a static pre-check. Self-referential primitive traversals now error instead of
  silently returning truncated results. Non-recursive traversals shortcut
  to level_0 for all types.

## Impact

- `fhirpath/src/main/java/.../collection/Collection.java`: Restructure
  `repeatAll()` method to add static type analysis gate.
- `fhirpath/src/test/java/.../dsl/RepeatAllFunctionDslTest.java`: Add test
  cases for primitive self-referential detection, update existing expectations.
- No public API changes. No configuration changes. The `maxExtensionDepth`
  setting continues to control Extension depth limiting.
