## Context

The `repeatAll()` method in `Collection.java` performs a static type analysis
gate before entering recursive tree traversal. It applies the traversal
expression twice — once to produce level_0, once more to produce level_1 — and
classifies behavior based on the FHIR type comparison between them.

The current classification covers five cases (empty level_1, same-type
primitive, same-type Extension, same-type non-Extension complex, and type
mismatch). However, it assumes `getFhirType()` always returns a populated
`Optional`. When both types are `Optional.empty()`, the equality check
`level0Type.equals(level1Type)` silently passes, treating two unknowns as
"consistent".

This is reachable through FHIR choice type expressions. A `MixedCollection`
(representing choice elements like `Observation.value[x]`) has
`fhirType = Optional.empty()`. The `$this` transform is pure identity —
it preserves the MixedCollection through both levels without triggering any
of the existing fail-fast paths (`traverse()`, `map()`, or `copyWith()`).

## Goals / Non-Goals

**Goals:**

- Reject traversal when either level_0 or level_1 has an indeterminate FHIR
  type, converting undefined behavior into a clear error message.
- Add test coverage for choice type interactions with `repeat()`/`repeatAll()`.
- Add test coverage for resource-level degenerate expressions.

**Non-Goals:**

- Supporting `repeat()`/`repeatAll()` on choice types. That would require
  resolving the choice to a concrete type first (via `ofType()`), which is
  already the recommended pattern.
- Changing the `MixedCollection` abstraction or making choice types carry FHIR
  type information.

## Decisions

### 1. Guard placement: before the existing type equality check

Add the indeterminate type guard immediately after the existing
`level0Type`/`level1Type` extraction (line 487-488) and before the
`equals()` comparison (line 489). This is the natural location — it extends the
existing classification with a sixth case rather than adding a separate concern.

**Alternative considered:** Guard at the entry point of `repeatAll()`, checking
the input collection's type before applying the transform. Rejected because
the indeterminate type only matters after the transform is applied — the input
collection could be a concrete type that produces a MixedCollection through
the transform, or vice versa.

### 2. Check `isEmpty()` on both types, not just equality

Use `level0Type.isEmpty() || level1Type.isEmpty()` rather than checking for a
specific pattern like both-empty. If either side is indeterminate, the
classification cannot proceed safely — we cannot determine whether types match,
whether the result is primitive, or whether it's an Extension.

### 3. Unify `allowPrimitiveSelfRef` into `allowSelfReference` for both type kinds

The existing `allowPrimitiveSelfRef` parameter only suppresses the error for
primitive self-referential traversal (returning level_0 for the caller to
deduplicate). For `repeat()` to work correctly with complex types like
`repeat($this)` on a resource, the same flag must also suppress
`errorOnDepthExhaustion` for complex non-Extension types. Rename the parameter
to `allowSelfReference` and use it to control both:

- Primitive case (line 508): return level_0 directly when `allowSelfReference`
  is true, instead of throwing.
- Complex non-Extension case (line 540): set
  `errorOnDepthExhaustion = !isExtensionTraversal && !allowSelfReference`,
  so depth exhaustion returns results silently for the caller to deduplicate.

This means `Patient.repeat($this)` produces depth-limited copies of Patient
which `repeat()` deduplicates to a single Patient — correct FHIRPath semantics.

**Alternative considered:** Two separate flags for primitive and complex cases.
Rejected because the rationale is identical in both cases — deduplication by the
caller guarantees termination, so depth exhaustion is not an error.

### 4. Error message distinguishes from other cases (renumbered)

Use a distinct error message mentioning "indeterminate FHIR type" to
differentiate from the existing "does not produce a consistent type" message
(type mismatch) and the "self-referential primitive traversal" message. This
helps users understand that the issue is an unresolved choice type, not a type
conflict.

### 5. Test with Observation for choice type coverage

Use `Observation` as the test resource for choice type scenarios because
`Observation.value[x]` is the most common and well-understood choice element.
Tests need a `createObservation()` helper (or equivalent) that produces an
Observation with a populated `value[x]` field to trigger the MixedCollection
code path.

### 6. Document expected error paths per expression

The test cases cover three distinct error paths:

| Error path                  | Example expression             | Guard that fires                 |
| --------------------------- | ------------------------------ | -------------------------------- |
| Indeterminate type (NEW)    | `value.repeatAll($this)`       | Empty fhirType guard             |
| Depth exhaustion (existing) | `repeatAll($this)` on resource | variantTransformTree depth limit |
| Earlier failure (existing)  | `repeatAll(value)` on choice   | MixedCollection.traverse()       |

Each test should assert the specific error type or message to document which
guard catches the expression.

## Risks / Trade-offs

- **[Low] False positives from the guard**: Any expression where the transform
  legitimately produces an indeterminate type would now be rejected. This is
  acceptable because indeterminate types in recursive traversal have no sound
  semantics — the user should resolve with `ofType()` first.
  → Mitigation: The error message guides users to resolve the choice type.

- **[Low] Test resource creation overhead**: Adding Observation-based tests
  requires a test helper. This is minor work and follows the existing pattern
  of `createPatient()` / `createQuestionnaire()`.
  → Mitigation: Reuse existing test infrastructure patterns.
