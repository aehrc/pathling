## Context

The `repeatAll()` function in `Collection.java` currently uses two mechanisms
for recursion control:

1. **Primitive shortcut** (`isPrimitive` check): If the traversal expression
   produces a primitive type, the function returns `level0` directly without
   entering the tree traversal. This bypasses all recursion detection.
2. **Analysis-time detection** (`UnresolvedTransformTree`): For complex types,
   Catalyst resolution compares SQL `DataType` at each level. Same-type
   depth exhaustion either errors (non-Extension) or silently stops (Extension).

The primitive shortcut is incorrect: expressions like `gender.repeatAll($this)`
are genuinely infinite but silently return a truncated result.

## Goals / Non-Goals

**Goals:**

- Detect self-referential primitive traversals statically and raise an
  immediate error with a descriptive message.
- Unify the "equivalent to select()" shortcut for both primitive and complex
  types when the traversal expression produces an empty collection on
  self-application.
- Enforce the invariant that recursive application of the traversal expression
  must produce either the same FHIR type or empty — anything else is an error.
- Distinguish error messages between static detection and analysis-time
  detection.

**Non-Goals:**

- Changing the Extension depth-limiting behavior (soft stop at configured
  depth).
- Distinguishing `repeatAll(extension)` from `repeatAll($this)` on Extension
  collections (accepted limitation — both get soft stop).
- Changing `UnresolvedTransformTree` logic.

## Decisions

### Decision 1: Static level_0/level_1 type analysis gate

**Choice**: Apply the traversal expression twice in `Collection.repeatAll()`
— once to the input (`level_0 = transform.apply(this)`) and once to its own
result (`level_1 = transform.apply(level_0)`) — to classify behavior before
any tree traversal.

**Rationale**: The `CollectionTransform` already supports self-application.
The FHIR type of the result is statically determined at evaluation time, so
comparing `level_0.getFhirType()` with `level_1.getFhirType()` (or checking
`level_1.isEmpty()`) gives a complete classification without needing Catalyst
resolution.

**Alternatives**: Could defer all detection to `UnresolvedTransformTree`, but
that requires routing primitives through the tree (adding complexity) and
produces less descriptive error messages.

### Decision 2: Four-way classification

The level_0/level_1 comparison produces exactly four outcomes:

| level_1 state            | level_0 type       | Action                                               |
| ------------------------ | ------------------ | ---------------------------------------------------- |
| Empty                    | any                | Return level_0 directly (equivalent to select)       |
| Same FHIR type           | primitive          | Error: self-referential primitive                    |
| Same FHIR type           | complex, Extension | `variantTransformTree(errorOnDepthExhaustion=false)` |
| Same FHIR type           | complex, other     | `variantTransformTree(errorOnDepthExhaustion=true)`  |
| Different non-empty type | any                | Error: inconsistent traversal types                  |

**Rationale**: This is exhaustive and each case has clear, distinct semantics.
The "return level_0" shortcut now applies to ALL types when level_1 is empty,
replacing the previous primitive-only shortcut.

### Decision 3: Distinct error messages

Three error contexts warrant different messages:

1. **Static primitive**: Self-referential primitive type detected at the
   `Collection.repeatAll()` level. Fires immediately, no Catalyst involved.
2. **Static inconsistent type**: Traversal expression produces different
   non-empty types across recursive application. Fires immediately.
3. **Analysis-time complex**: Same SQL `DataType` depth exhausted in
   `UnresolvedTransformTree`. Fires during Catalyst analysis for complex types
   where structural convergence can only be determined after schema resolution.

The existing message in `UnresolvedTransformTree` ("Infinite recursive traversal
detected.") remains unchanged.

### Decision 4: FHIR type comparison for level_0/level_1

**Choice**: Use `FHIRDefinedType` equality to compare level_0 and level_1
types. `EmptyCollection` is detected via the existing `isEmpty()` static check.

**Rationale**: FHIR type drives the semantic behavior. SQL type comparison is
appropriate for `UnresolvedTransformTree` (where schema divergence matters) but
not for the static gate (where we need to know if the traversal is
self-referential in FHIR terms).

## Risks / Trade-offs

**`Patient.extension.repeatAll($this)` not caught as infinite** →
Accepted limitation. The level_1 check sees Extension→Extension (same FHIR
type, complex, Extension) and routes to soft stop. This is genuinely infinite
but indistinguishable from the legitimate `repeatAll(extension)` case at the
FHIR type level.

**Double application of transform** → The `transform.apply(level_0)` call
adds one extra static evaluation. This is negligible compared to the Spark
query planning that follows, and avoids entering the tree traversal entirely
for the common non-recursive case.
