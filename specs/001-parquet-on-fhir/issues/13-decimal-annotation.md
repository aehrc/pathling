# 13 — Decimal numeric annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-021, FR-022, FR-023. Old tasks: T050a, T064, T086a, T095a.

**What to build:** A decimal stored as text carries a numeric annotation beside it, and the engine uses it as a fast path for comparison, arithmetic, ordering and decoding. The engine chooses the fast path from the schema, never per row. The lexical path stays the fallback and stays correct.

**Where the kind lives (decision 135).** The numeric kind is defined once, in the kinds package beside `fhir-schema`'s annotation API, and the engine's own computation of a decimal's value moves onto it. It is stored by the annotation step of ticket 11, not by the reader.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] A test shows a decimal carries the annotation beside its stored text, at the agreed type and position (T050a).
- [ ] Decimal comparison, arithmetic and ordering with the annotation give results equal to the computed path T086 pins (T086a).
- [ ] Decoding takes the annotation where the schema carries it (T095a). With the annotation disabled, the suite passes on the lexical path.
