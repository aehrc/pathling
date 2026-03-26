## Context

Pathling already implements `repeatAll()` which performs recursive tree traversal without deduplication. The FHIRPath standard `repeat()` function has identical recursive semantics but deduplicates results using the `=` (equals) operator.

The `UnionOperator` already implements equality-based array deduplication for the `|` operator, supporting both default SQL equality (`array_distinct`) and custom equality (`arrayDistinctWithEquality`) via the `ColumnEquality` comparator interface. This infrastructure can be reused.

## Goals / Non-Goals

**Goals:**

- Implement `repeat()` as defined in the FHIRPath specification.
- Reuse `repeatAll()` for recursion and `UnionOperator`-style dedup for equality.
- Pass the existing YAML reference tests for `repeat`.

**Non-Goals:**

- Per-level deduplication (BFS queue with set tracking) — post-collection dedup is sufficient.
- Deduplication of complex types (BackboneElement, etc.) — synthetic fields like `_fid` make struct equality impractical, and tree-structured FHIR data doesn't produce duplicates in practice.
- Decimal normalization before dedup — unlikely to be needed since all elements come from the same `repeatAll` traversal with a consistent schema. Can be added if tests reveal issues.

## Decisions

### 1. Implement as repeatAll + post-dedup

**Decision**: `repeat()` calls `repeatAll()` then deduplicates the result array.

**Rationale**: The spec defines `repeat` with a BFS queue that deduplicates on insertion. In a columnar engine, per-element queue iteration isn't feasible. Post-collection dedup produces the same result set (order is undefined per spec). This avoids duplicating the complex `variantTransformTree` machinery.

**Alternative considered**: Level-by-level dedup during traversal — rejected as it would require modifying `variantTransformTree` internals for minimal benefit.

### 1a. Allow primitive self-referential traversal via repeatAll parameter

**Decision**: Add an `allowPrimitiveSelfRef` boolean parameter to `Collection.repeatAll()`. When `true`, self-referential primitive traversal returns level_0 instead of throwing an error. `repeat()` passes `true` (dedup handles termination); `repeatAll()` continues to pass `false` (error, since without dedup it would loop infinitely).

**Rationale**: In `repeat()`, expressions like `(1 | 2).repeat('item')` are valid — the projection produces `['item', 'item']`, dedup collapses to `['item']`, and re-evaluation finds no new items. The error in `repeatAll()` exists because without dedup there's no termination. Adding a parameter keeps the analysis logic centralized in `repeatAll()` rather than duplicating it in `repeat()`.

**Alternative considered**: Having `repeat()` do its own level_0/level_1 analysis and short-circuit the primitive case before calling `repeatAll()` — rejected to avoid duplicating analysis logic.

### 2. Only deduplicate Equatable types

**Decision**: Check `instanceof Equatable` on the result collection. If true, use the collection's comparator for dedup. If false, return as-is.

**Rationale**: `Equatable` covers all FHIRPath primitive types (String, Integer, Boolean, Decimal, Date, DateTime, Time), plus Coding and Quantity which have custom equality semantics. Complex backbone types don't implement `Equatable` and contain synthetic fields that prevent meaningful equality. The YAML test cases that exercise dedup all involve primitive or Quantity types.

### 3. Reuse UnionOperator's dedup pattern

**Decision**: Extract or replicate the `deduplicateArray` pattern from `UnionOperator` — check `comparator.usesDefaultSqlEquality()` to choose between `array_distinct` and `arrayDistinctWithEquality`.

**Rationale**: This is the established pattern for FHIRPath equality-based dedup in the codebase. It correctly handles Quantity unit conversion, Coding system/code matching, and temporal types.

## Risks / Trade-offs

- **Post-dedup vs per-level dedup**: Post-dedup may include elements in the recursion that would have been filtered at insertion time in a true BFS queue. For tree-structured FHIR data this is irrelevant (no duplicates during traversal). For contrived cases with primitives, the depth limit prevents infinite loops regardless, and post-dedup produces the correct final set. [Risk: minimal] → Accepted.
- **No complex type dedup**: If a user has a genuine need to deduplicate complex elements from `repeat()`, it won't happen. [Risk: low] → Can be revisited if real use cases emerge.
