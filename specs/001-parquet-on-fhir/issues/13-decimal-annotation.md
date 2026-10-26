# 13 — Decimal numeric annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-021, FR-022, FR-023. Old tasks: T050a, T064, T086a, T095a.

**What to build:** A decimal stored as text carries a numeric annotation beside it, and the engine uses it as a fast path for comparison, arithmetic, ordering and decoding. The engine chooses the fast path from the schema, never per row. The lexical path stays the fallback and stays correct.

**Where the kind lives (decision 135).** The numeric kind is defined once, in the kinds package beside `fhir-schema`'s annotation API, and the engine's own computation of a decimal's value moves onto it. It is stored by the annotation step of ticket 11, not by the reader.

**Placement (decision 135, part 4).** The numeric annotation is `_<element>.__numeric`, a field of the decimal's metadata group, so the group is stored for it before ticket 17 stores ids and extensions. The engine reads it by pairing the element with `_x`, naming only the annotation fields it needs, so that the read prunes (`evidence/annotation-pruning.md`).

**The annotation's type (owner, 2026-10-26).** `__numeric` takes the engine's System.Decimal type, `DecimalCollection.DECIMAL_TYPE` (today `DECIMAL(32,6)`), named once rather than restated, so that reading the annotation gives exactly what decoding the text gives and the results T086 pins do not change. Until FHIR and System primitives are separate types, every decimal result in the engine is a System.Decimal, so the engine reads the value from `__numeric` where the schema carries it and decodes the text otherwise. The text stays the stored value: egress writes it, and the annotation is only the engine's fast path to the number. Once FHIR and System types are separated, a FHIR.decimal may be returned from the stored text, and `__numeric` stays the fast path for its conversion to System.Decimal. That pays off fully only once the text preserves the source's lexical form (OQ-1, ticket 16).

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] A test shows a decimal carries the annotation beside its stored text, at the agreed type and position (T050a).
- [ ] Decimal comparison, arithmetic and ordering with the annotation give results equal to the computed path T086 pins (T086a).
- [ ] Decoding takes the annotation where the schema carries it (T095a). With the annotation disabled, the suite passes on the lexical path.
