# 42 — Reconsider how an annotation kind's shared computation is processed

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-005, FR-022, FR-023. Raised from ticket 14's review of the System date (owner, 2026-10-28).

**What to decide:** whether the fields of an annotation kind that one computation produces are kept together as a structure so that the computation runs once, or are read one by one with the shared computation cached.

Today the structure carries the guarantee. The date range kind computes `__start` and `__end` from one parse, `DateRangeKind.compute`, which returns both as a structure. `ColumnFunctions.resolveFieldsOrElse` reads the stored fields as one structure, or falls back to that one computation, so that the parse is not repeated for each field. The engine's System date then keeps the range whole, as `struct(value, range{__start, __end})`, rather than as flat fields. The quantity kind does the same with `__canonical_exact`, and `QuantityEncoding.compareCanonical` compares two canonical forms in one function, so that a computed form is not read once for its code and again for its value.

Reading a field of the computed structure more than once repeats the computation. `SqlFunctions.let` binds only an operand that is not deterministic, and Catalyst does not eliminate common subexpressions inside a higher-order function's lambda, where repeating values are traversed. The alternative is to bind it with `bindOnce`, which wraps it in `element_at(transform(array(..)))`.

The owner asks whether caching, or memoising, the shared part would serve better than imposing the structure: each field could then be read on its own, the System date could be flat, and a later kind would not need its own grouping.

Consider:

- Where the cache would live: in the kind's function, such as a cache of the last input in each task, or in the plan.
- Whether it holds inside lambdas and under whole-stage code generation, and what it costs on a row that reads no field.
- That the computation must stay deterministic, and a traced operand must still fire once (#2594).
- What it would change: `resolveFieldsOrElse`, the System date's `range`, `compareCanonical`, and `bindOnce` in `TemporalComparator` and `DateMatcher`.

**Blocked by:** 14

**Status:** needs-triage

- [ ] The owner decides between the structure and caching the shared computation, and the decision is recorded.
- [ ] If caching: a measurement in `evidence/` shows the shared computation runs once per value when its fields are read separately, singular and repeating, and the structures it replaces are removed.
