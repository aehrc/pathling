# 12 — Quantity annotations, UCUM-only canonicalisation, codegen-fallback measurement

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-005, FR-021, FR-022, FR-023. Old tasks: T054, T066, T087a, T096a (with a repeat of T100o's measurement), T096b and T096c; decisions 77, 92, 103, 126.

**What to build:** A stored quantity carries both canonical annotations, and the engine uses them as a fast path.

1. **Emit two annotations, in this order (T054, T066).**
   - `__<field>_canonical`: the specification's own, at `DECIMAL(38,6)`.
   - `__<field>_canonical_exact`, immediately after it: the exact value plus the canonicalised unit code. Without the unit, one metre and one second compare equal.
   - `fhir-schema` fixes the names and positions. The Spark types are settled here.
2. **Use them in the engine (T096a, T087a).** Quantity handling prefers `_canonical_exact`, falls back to `_canonical`, and computes canonicalisation only where neither exists. This includes `QuantityEncoding.decodeStored`, so the UDF calls leave the decode where the annotation is present. Repeat T100o's measurement afterwards.
3. **Canonicalise only UCUM quantities in comparison (T096b).** Apply UCUM canonicalisation only where the system is UCUM, as quantity search already does. Any existing test whose expected value changes is listed for the owner's approval before it is updated.
4. **Measure the codegen fallback (T096c).** Measure what the quantity `!=` costs by falling out of whole-stage code generation, and check `=`. Measure against plans that already fall back for Coding filtering (decision 126), with enough rows for the per-row cost to show. Report to the owner before rewriting anything. If the fallback matters engine-wide, raise that in the report.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests pin both annotations: their presence, types and order, and that quantities orders of magnitude apart do not compare equal on `_exact`.
- [ ] Comparison with annotations gives the same results as the computed path T087 pins.
- [ ] Each annotation can be disabled, and the engine stays correct without it.
- [ ] T100o's measurement is repeated and recorded in the evidence.
- [ ] The UCUM-only change is in place, and changed expectations were approved by the owner.
- [ ] The T096c measurement is recorded and reported to the owner. Any rewrite happens only on the owner's go-ahead.
- [ ] The benchmark (ticket 10) is re-run, with plans saved.
