# 50 — Decimals and System.Quantity in the engine

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-004, FR-022, FR-023, FR-059. Split from ticket 43 (owner, 2026-11-11), which built the accessor model for dates. Settled by the annotations design, `design/annotations-in-the-engine.md` §4.1 to §4.5 and §8, and decisions 142, 147, 150 and 152. Examples: `design/annotations-by-example.md`.

**What to build:** The accessor model of ticket 43, for decimals and quantities. Ticket 43 built `ResolveOrNone`, the accessor `AnnotationOrElse`, the merge's intersection of annotation fields and the comparison key; this ticket applies them to the two remaining annotated types.

- **Element form (decisions 141 and 152), for decimals.** A FHIR primitive whose type has an annotation kind is `struct(value, <named stored annotation fields>)`, flat: `__numeric` for a decimal. The shape is fixed by the type, not the data. The raw `_x` is never carried.
- **Accessors (design §4.3), for decimals.** A decimal keeps its stored text as `value`; its number is `numeric(e)`, the stored `__numeric` or else the parsed text, deferred until it is read.
- **Conversion (decision 142, D8, as clarified 2026-11-09), for decimals.** Every operation that accepts System types converts through one shared step. For decimals it is new, because traversal yields the number today, so each place that uses `DecimalCollection`'s column directly gains it.
- **Decimal detection (decision 147; moved from ticket 43, which had it from ticket 41).** A decimal is classed as stored or computed from the collection's definition, as `QuantityCollection.holdsStoredQuantities` does. A union is stored only if every operand is; `valueDecimal | 1.5` and `1.5 | valueDecimal` give the same result.
- **A stored decimal's text (decision 147; handed on from ticket 41, owner, 2026-11-10; moved from ticket 43).** `toString()` on a stored decimal returns its stored text, the element's `value`. Keys and comparisons stay numeric, so `1.0 = 1.00` is true and `(1.0 | 1.00)` is one element.
- **Comparison keys (decision 151), for decimals.** A primitive's key is its System value, whatever its annotation fields: a decimal deduplicates on `numeric(e)`, not on its text. Today `DecimalComparator` is compared with `array_distinct` on the number.
- **Merges (decision 142, D9, and decision 152).** An operation that converts before it compares, such as a merge or `toQuantity()`, keeps the annotation fields it has; where they are absent the accessor computes from the converted value.
- **System.Quantity (decision 150, as amended 2026-11-10; moved from ticket 42 to 43, owner, 2026-11-10, and from 43 here, 2026-11-11).** System.Quantity becomes `{value, unit, code, system}`, with `value` a `DECIMAL(38,6)` and no `id`, `comparator` or `_value`. Its canonical annotations, `__canonical_code` and `__canonical_value_exact`, sit flat beside them only where they are available without computing: copied from a stored quantity whose schema carries them, or constant for a literal. Otherwise they are computed where a comparison reads them, from the most exact source: the stored text for a stored quantity, the `DECIMAL(38,6)` value for arithmetic or a quantity converted before it is compared. A System.Quantity returned to a caller as a column has the four content fields only. Ticket 42 left the interim shape: today's fields with the two canonical annotations flat and always filled, which `SystemQuantityShapeTest` pins; `contracts/engine-semantics.md` §Quantities describes both.
- **Output (decision 142, D10, and design §4.5).** A primitive leaves the engine as its value only. Its annotation fields are never output, and neither are a System.Quantity's canonical fields.

**Handed over from ticket 42 (owner, 2026-11-10).** Gate 3 of ticket 42: decision 150's shape gives System.Quantity more than one Spark type, and the engine assumes one. `FhirPathType.QUANTITY` has one SQL type, the conversion UDFs return `QuantityEncoding.dataType()`, `toUnit` and `ConversionLogic` cast nulls to it, and `CombiningLogic` combines System operands without reconciling them. A stored quantity with a comparator has no System form, so it leaves a union, and `toString()` and unit conversion of a stored value work from its text, not the `DECIMAL(38,6)` value.

**Not in this ticket:** the view output of a stored decimal's text, which is ticket 16; the emission shapes and the level tests, which are ticket 51.

**Blocked by:** 43.

**Status:** ready-for-agent

- [ ] A test runs each annotated type over annotated and unannotated data and gets the same results, including values whose text and number order differently (`"10"` against `"9"`, `1.0` against `1.00`).
- [ ] System.Quantity has the shape of decision 150, and a returned one has its four content fields only; `SystemQuantityShapeTest` asserts both.
- [ ] `toString()` on a stored decimal returns its text, and keys and comparisons stay numeric (`1.0 = 1.00` is true, `(1.0 | 1.00)` is one element).
- [ ] Stored and computed decimals are told apart by the collection's definition, and a union is stored only if every operand is; `valueDecimal | 1.5` and `1.5 | valueDecimal` give the same result.
- [ ] The baseline plan test of a stored quantity merged with a literal (`AnnotationPlanBaselineTest`) gives the result of `design/annotations-by-example.md` §5.1.
