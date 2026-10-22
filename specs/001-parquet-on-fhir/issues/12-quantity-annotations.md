# 12 — Quantity annotations, UCUM-only canonicalisation, codegen-fallback measurement

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-005, FR-021, FR-022, FR-023. Old tasks: T054, T066, T087a, T096a (with a repeat of T100o's measurement), T096b and T096c; decisions 77, 92, 103, 126.

**What to build:** A stored quantity carries both canonical annotations, and the engine uses them as a fast path.

1. **Emit two annotations, in this order (T054, T066).**
   - `__canonical`, inside the quantity's structure (decision 135, part 4): the specification's own, at `DECIMAL(38,6)`.
   - `__canonical_exact`, immediately after it: the exact value plus the canonicalised unit code. Without the unit, one metre and one second compare equal.
   - `fhir-schema` fixes the names and positions. The Spark types are settled here.
2. **Use them in the engine (T096a, T087a).** Quantity handling prefers `_canonical_exact`, falls back to `_canonical`, and computes canonicalisation only where neither exists. This includes `QuantityEncoding.decodeStored`, so the UDF calls leave the decode where the annotation is present. Repeat T100o's measurement afterwards.
3. **Canonicalise only UCUM quantities in comparison (T096b).** Apply UCUM canonicalisation only where the system is UCUM, as quantity search already does. Any existing test whose expected value changes is listed for the owner's approval before it is updated.
4. **Measure the codegen fallback (T096c).** Measure what the quantity `!=` costs by falling out of whole-stage code generation, and check `=`. Measure against plans that already fall back for Coding filtering (decision 126), with enough rows for the per-row cost to show. Report to the owner before rewriting anything. If the fallback matters engine-wide, raise that in the report.
5. **Give the System.Quantity shape the canonical annotations.** Ticket 03 took the legacy canonical companions off an engine-built System.Quantity, so a quantity literal is now canonicalised once per row wherever it is compared, where before its canonical form was a constant. Restore that by making the two Parquet on FHIR canonical annotations part of the fixed System.Quantity shape, which amends FR-059.
   - A literal fills them in Java when the plan is built, so they reach the plan as constants.
   - A stored quantity taken as a System one copies its stored annotations where the schema has them, and computes them otherwise.
   - A conversion or a promoted number computes them. Quantity arithmetic, when it is built, fills them from the canonical form it computes anyway.
   - Comparison, union deduplication and the other set operations read them rather than canonicalising again. Carried in the struct, they survive unions, arrays and lambdas, which metadata kept on the collection would not.
   - Where canonicalisation is computed, compute it once. Today `QuantityEncoding.canonicalValue` and `canonicalCode`, and `QuantityMatcher`, call two UDFs that each run the full `Ucum.getCanonical` on the same value and code. Replace them with one UDF returning the canonical value and code together, bound once and read for both fields. The UDF also takes the system and maps a definite calendar duration to its UCUM equivalent in Java, replacing the `when` chain `QuantityEncoding.ucumEquivalentOf` puts into the plan, which is currently copied once per canonicalisation call. `QuantityMatcher` uses the same shared canonicalisation and decode from `QuantityEncoding`, and so the annotations' fast path, instead of its own UDF calls and its `value()` helper, which repeats `QuantityEncoding.decodeValue`.
   - Where a promoted number is encoded, `QuantityEncoding.withoutTrailingZeros` strips trailing zeros from the decimal's text with nested `regexp_replace` calls, which duplicates what the `DecimalToLiteral` UDF already does for `ConversionLogic.convertToString`. Reuse the one formatting rule, so that a quantity's value and a decimal's string form cannot drift apart, as the promoted-number path is reworked to fill the annotations.
   - Decide how they leave the engine: whether rendering and the columns returned to callers keep or drop them, and whether the FR-021 annotation switches govern them.

**Where the kind lives (decision 135).** The quantity kind is defined once, in the kinds package beside `fhir-schema`'s annotation API, and `fhir-schema` gains its own UCUM dependency, since `encoders` holds the current one and FR-051 does not let it move. `QuantityEncoding` and `QuantityMatcher` move onto the kind, so the engine's fallback and the stored annotation are one computation. The one UDF returning the canonical value and code together, in item 5, is that kind's computation. The kind is stored by the annotation step of ticket 11, not by the reader.

**Blocked by:** 11

**Status:** ready-for-agent

- [ ] Tests pin both annotations: their presence, types and order, and that quantities orders of magnitude apart do not compare equal on `_exact`.
- [ ] Comparison with annotations gives the same results as the computed path T087 pins.
- [ ] Each annotation can be disabled, and the engine stays correct without it.
- [ ] T100o's measurement is repeated and recorded in the evidence.
- [ ] The UCUM-only change is in place, and changed expectations were approved by the owner.
- [ ] The T096c measurement is recorded and reported to the owner. Any rewrite happens only on the owner's go-ahead.
- [ ] An engine-built System.Quantity carries the canonical annotations, FR-059 is amended to say so, and a plan test shows no canonicalisation call on the literal side of a comparison.
