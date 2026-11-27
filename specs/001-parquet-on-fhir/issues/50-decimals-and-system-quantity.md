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
- **System.Quantity (decision 150, as amended 2026-11-10; moved from ticket 42 to 43, owner, 2026-11-10, and from 43 here, 2026-11-11).** System.Quantity becomes `{value, unit, code, system}`, with `value` a `DECIMAL(38,6)` and no `id`, `comparator` or `_value`. Its canonical annotations, `__canonical_code` and `__canonical_value_exact`, sit flat beside them only where they are available without computing: copied from a stored quantity whose schema carries them, or constant for a literal. Otherwise they are computed where a comparison reads them, from the quantity's `DECIMAL(38,6)` value (decision 153, owner, 2026-11-12; first planned from the most exact source, the stored text for a stored quantity). A System.Quantity returned to a caller as a column has the four content fields only. Ticket 42 left the interim shape: today's fields with the two canonical annotations flat and always filled, which `SystemQuantityShapeTest` pins; `contracts/engine-semantics.md` §Quantities describes both.
- **Output (decision 142, D10, and design §4.5).** A primitive leaves the engine as its value only. Its annotation fields are never output, and neither are a System.Quantity's canonical fields.

**Handed over from ticket 42 (owner, 2026-11-10).** Gate 3 of ticket 42: decision 150's shape gives System.Quantity more than one Spark type, and the engine assumes one. `FhirPathType.QUANTITY` has one SQL type, the conversion UDFs return `QuantityEncoding.dataType()`, `toUnit` and `ConversionLogic` cast nulls to it, and `CombiningLogic` combines System operands without reconciling them. A stored quantity with a comparator has no System form, so it leaves a union, and `toString()` and unit conversion of a stored value work from its text, not the `DECIMAL(38,6)` value.

**Not in this ticket:** the view output of a stored decimal's text, which is ticket 16; the emission shapes and the level tests, which are ticket 51.

**Blocked by:** 43.

**Status:** done

- [x] A test runs each annotated type over annotated and unannotated data and gets the same results, including values whose text and number order differently (`"10"` against `"9"`, `1.0` against `1.00`).
- [x] System.Quantity has the shape of decision 150, and a returned one has its four content fields only; `SystemQuantityShapeTest` asserts both.
- [x] `toString()` on a stored decimal returns its text, and keys and comparisons stay numeric (`1.0 = 1.00` is true, `(1.0 | 1.00)` is one element).
- [x] Stored and computed decimals are told apart by the collection's definition, and a union is stored only if every operand is; `valueDecimal | 1.5` and `1.5 | valueDecimal` give the same result.
- [x] The baseline plan test of a stored quantity merged with a literal (`AnnotationPlanBaselineTest`) gives the result of `design/annotations-by-example.md` §5.1.

## Comments

### Built, 2026-11-11 (branch issue/2832_50_decimals-and-quantity)

**Decimals.** A traversed decimal is `struct(value, __numeric)`, its stored text and, where the
schema carries it, its numeric annotation, read by `ResolveOrNone` with nothing computed
(`StoredDecimal`). Its number is `StoredDecimal.numeric`, `annotation_or_else(e, "__numeric",
decode(e.value))`. A decimal is stored where its collection has a definition, and computed
otherwise; `DecimalCollection.toEngineForm` converts a stored one to its number, and every
operation that computes with decimals goes through it. A union of stored decimals stays stored,
merged under `StoredDecimal.STRUCTURE`; any computed operand makes the union computed, whichever
side it is on. Stored decimals are compared and deduplicated by number (`StoredDecimalComparator`,
whose deduplication key is the number). `toString()` on a stored decimal returns its stored text.
View output and `getOutputColumn` stay the number, until ticket 16.

**System.Quantity.** `{value DECIMAL(38,6), unit, code, system}`, with `__canonical_code` and
`__canonical_value_exact` flat beside them only where available: kept from a stored quantity whose
schema has them, or constants for a literal. A function result (`toQuantity()`, a unit conversion,
a promoted number) carries none (rule A). A stored quantity with a comparator has no System form,
and converts to null, so it leaves a union or combination with a System quantity. Merges intersect
the canonical fields (`QuantityEncoding.STRUCTURE`). A returned System quantity is its content
only (`QuantityCollection.getOutputColumn`). The canonical accessor (`QuantityEncoding.canonical`)
reads the stored annotation, or else computes from the System.Decimal value, for a stored
quantity and a System one alike (decision 153). The formatting UDF takes the quantity as text
(`QuantityEncoding.asText`), so a stored quantity's `toString()` gives its stored text, and a
System quantity's string drops trailing fractional zeros, as a decimal's does. The conversion UDF
takes the text of the System.Decimal value (`QuantityEncoding.asSystemText`), so `toQuantity(unit)`
converts that value (owner, 2026-11-12).

**Departures, as built.**

- `StoredQuantityComparator` is deleted. `QuantityComparator` reads either form, with the canonical
  form of each operand's System.Decimal value (owner, 2026-11-12; see below).
- Three places bypass the shared conversion step (`toEngineForm`): a quantity comparison
  (unification no longer converts quantities); `toString()` on a stored decimal or quantity, so
  that it gives the stored text; and `convertsTo*()`, which converts decimals only, so that a
  bounded quantity stays a bound. The fourth, `toQuantity(unit)` on a stored quantity, which
  converted from the stored text, is removed (owner, 2026-11-12, decision 153): it converts the
  System.Decimal value, as every computation does.
- `FlatStructure` carries a nested field order, so that a merge keeps `__canonical_value_exact` as
  `{value, scale}`, which the comparison reads by position.
- A NUMBER search on a decimal matches against its number (`SearchColumnBuilder.matchedColumn`); a
  quantity search still reads the stored quantity.
- The terminology functions read a quantity's Coding from the quantity as held, so a bound still
  gives the Coding of its unit.

**Behaviour changes accepted by the owner (2026-11-11).**

- `toString()` on a stored decimal returns the stored text, which is what JSON ingest's double round
  trip stored (decision 68), so it can be in double or exponent form (`"10.0"`, `"1.0E-7"`). Ticket
  16, which keeps the lexical form, changes the text.
- `fhirPathToColumn` returns null for a stored quantity with a comparator, which has no System
  form. What the API returns in general is ticket 40's to decide.
- A System quantity's value is held at DECIMAL(38,6): `'0.1234567891 \'mg\''.toQuantity()` rounds,
  and a unit conversion whose result is below 1e-6 carries no canonical form to compare by (rule A).
- `$fhirpath` and `evaluateFhirPath` return a System quantity's value at six fractional digits, as
  they return a computed decimal.

**Tests.** `AnnotatedTypesParityTest` (29) runs dates, decimals and quantities over annotated and
unannotated data, with the stored texts set directly. `SystemQuantityShapeTest` asserts the engine
form per producer and the four-field return. Changed expectations: the approved lists (owner,
2026-11-11) in `SystemQuantityShapeTest`, `AnnotationPlanBaselineTest`, `DecimalCollectionTest`,
`StoredQuantityTest`, `QuantityNumericAnnotationTest` and `EvaluateFhirPathTest`; and, at the
agent's discretion, test helpers that read a decimal as a caller is given it (no value changed),
plan-text assertions that the decimal's struct changed, and the encoding unit tests, rewritten for
the new shapes. New pins: a System quantity's string drops trailing zeros, and the API returns a
computed decimal at six fractional digits.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at `eb53e1a634`, all green: utilities
40, fhir-schema 97, encoders 620, io 797 (1 skipped), legacy-export 38, terminology 926, fhirpath
8014 (888 skipped), library-api 380. `AnnotationFreeSuiteTest` passes within it. Python, R and the
server were not run.

### Review fixes, 2026-11-11 (owner)

- **A converted stored quantity compares the same over annotated and unannotated data.** A stored
  `1.0000001 g` was counted twice in `(valueQuantity | 1 'g')` over annotated data and once over
  unannotated data. The first fix computed the canonical form from the stored text at conversion,
  as an exception to D8. The owner replaced it on 2026-11-12 (decision 153; see below): the
  canonical form is computed from the System.Decimal value, and conversion computes no annotation
  again.
- **A stored decimal the engine's System.Decimal cannot hold is absent again,** as before the
  element held its text: empty where it is singular, and a null item under a repeating parent or in
  a repeating field (`StoredDecimal`, `DefaultRepresentation.traverseStored`). Its number is tested
  as it is traversed.
  The owner kept the null item (2026-11-11), so `DecimalCollectionTest`'s count of 3 stands.
- **Departure: a stored quantity without a code has no System form** (FHIR R4 FHIRPath binding,
  §2.9.1.3: the mapping applies only if the Quantity has a UCUM code), so `toQuantity('mg')` of
  `{value 5, unit 'mg', system UCUM}` is empty, where it gave `5 'mg'` before this ticket
  (`QuantityValue.toUnit`).
- `AnnotatedTypesParityTest` (40) also covers ordering of two stored quantities, both of the
  above, and a quantity without a code. No R4 element is a repeating decimal, so `fromStoredArray`
  is covered only by `StoredNumericAnnotationTest`'s synthetic data. The YAML harness's check of a
  quantity's field set is deferred to ticket 40.
- `QuantityEncoding.ID_COLUMN` is removed, and the `isSystemQuantity` javadoc is corrected.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at `f5ac69f57c`, all green: utilities
40, fhir-schema 97, encoders 620, io 797 (1 skipped), legacy-export 38, terminology 926, fhirpath
8025 (888 skipped), library-api 380. Python, R and the server were not run.

### Canonical form from the System.Decimal, 2026-11-12 (owner)

**The governing rule (decision 153): every computation, comparison included, operates on the
System.Decimal value at `DECIMAL(38,6)`.** Values with more than six fractional digits are rare,
and the scale will move to eight under aehrc/pathling#2893, Consider 8 fractional digits for
System.Decimal; everything follows the one scale. A value outside the supported range is not
available (null) for computation and wherever it is used as a System.Decimal.

So a quantity's canonical form is a function of its System.Decimal value, its `value` rounded half
up to `DECIMAL(38,6)` as the engine decodes it, and its unit, at ingest and in the engine.
`__canonical_value_exact` only keeps canonicalisation itself from losing digits (`1 ns` is
0.000000001 s). A value the System.Decimal cannot hold has no canonical form.

- `DecimalNumericKind.decodeValue` decodes a known number as `DecimalNumericKind.decode` does in the
  plan. `QuantityCanonicalKind` canonicalises the decoded value, so the ingest annotation and the
  engine's fallback agree by construction.
- A quantity literal canonicalises from its value rounded the same way, so its value and canonical
  form agree. A known limitation: `0.000000001 's'` is 0 s, so `1 'ns' = 0.000000001 's'` is false,
  while `1 'ns'` itself is exactly 0.000000001 s.
- A number or quantity search rounds its search value the same way (`NumericMatchingSupport.systemValue`):
  `eq` and `ne` take the implicit range of the rounded value, so `eq0.0000002` searches as
  `eq0.000000`; `gt`, `ge`, `lt` and `le` compare the rounded value; a quantity search canonicalises
  it. Owner decisions (2026-11-12, decision 153): a search value outside the range matches nothing,
  under `ne` too; an integer element's search rounds its search value before the integer `eq`/`ne`
  rule, so `eq1.0000001` finds 1 and an out-of-range value matches nothing (`NumberMatcher`); and a
  quantity search value is rounded before it is canonicalised, so `eq0.0002|…|mg` does not find a
  stored `0.0000002 g`. A range boundary finer than the System.Decimal, such as the
  0.0000005 either side of a six-digit value, is raised onto its grid, which keeps the same values in
  the range and keeps Spark from widening the element to `DECIMAL(38,7)`, where a value with 32
  integer digits no longer fits.
- `QuantityEncoding.fromStored` is back to `resolve_or_none`: it keeps the canonical fields the
  schema has, and computes none (D8). The 2026-11-11 exception and its design amendments are
  reverted, and §4.2 of `design/annotations-in-the-engine.md` and §4.3 and §5 of
  `design/annotations-by-example.md` state the new rule.
- A stored quantity whose value does not decode, such as `1.0E33 g`, has no System form: it
  converts to null, and formats and converts to no unit, so its `toString()` is empty, it is absent
  where it is singular, and it is a null item under a repeating parent, as its value is. Before, it
  compared by a canonical form computed from its text, so `valueQuantity > 1 'g'` was true.
- The undecodable-decimal rule is now documented: traversal computes no annotation, but tests
  whether a decimal's number decodes (design §4.1, by-example §3.1 and §5). `iif` was listed as a
  merge, but the engine does not implement it; the design no longer lists it.

**Tests.**

- Approved changes (owner, 2026-11-12):
  - `AnnotatedTypesParityTest`, o2 (stored `1.0000001 g`): `(valueQuantity | 1 'g').count()` 2 to 1,
    `valueQuantity.toQuantity() = 1 'g'` false to true, and `valueQuantity > 1 'g'` and
    `valueQuantity > 1000 'mg'` true to false;
  - `SystemQuantityShapeTest` and `AnnotationPlanBaselineTest` back to no canonical fields on a merge
    over unannotated data;
  - `QuantityCollectionTest` (stored `0.0000002 kg`, now 0 kg): `= 0.2 'mg'` and `> 0.1 'mg'` true
    to false, in the test renamed `canonicalisationIsOfTheSystemDecimalValue`;
  - `QuantitySearchPrecisionTest`, renamed `searchMatchesTheSystemDecimalValue`, the same stored
    value: `eq0.0000002|…|kg` and `eq2e-7|…|kg` still find q2, now because both are 0 kg;
    `eq0.2|…|mg` and `eq200|…|ug` find nothing; `gt0.1|…|mg` finds q1 alone; `lt0.3|…|mg` still finds
    q2.
- At the agent's discretion: `AnnotatedTypesParityTest`'s own literal pins, created in this ticket,
  now expect `valueQuantity = 0.000000001 's'` and `1 'ns' = 0.000000001 's'` to be false, with
  `0.000000001 's' = 0 's'` true.
- New: `AnnotatedTypesParityTest` adds `toQuantity('g')` and `value = 1` for o2, the undecodable null
  item through `distinct`, ordering, union, `combine` and `toString`, and a third dataset with a
  quantity of `1.0E33 g`, quantities whose texts round to one System.Decimal, and quantities in
  nanoseconds. `SearchValueSystemDecimalTest` searches decimals and quantities with values of more
  than six fractional digits, out of range, and with 32 integer digits and six fractional ones.
  `CanonicalDecimalAgreementTest` checks the known-number decoding against Spark's and the
  canonical form of a text against that of its decoded value. `DecimalNumericKindTest` and
  `QuantityCanonicalKindTest` pin the rounding and the exactness of a small unit.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at `7affc760b3`, all green:
utilities 40, fhir-schema 100, encoders 620, io 797 (1 skipped), legacy-export 38, terminology 926,
fhirpath 8090 (888 skipped), library-api 380. No YAML reference test or exclusion baseline changed.
Python, R and the server were not run. After the integer search rounding (owner, 2026-11-12),
`SearchValueSystemDecimalTest` gained nine integer cases, and the same command at `8568bee87d` was
all green: utilities 40, fhir-schema 100, encoders 620, io 797 (1 skipped), legacy-export 38,
terminology 926, fhirpath 8099 (888 skipped), library-api 380.

**Final review fixes (owner, 2026-11-12).** `toQuantity(unit)` and `convertsToQuantity(unit)` convert
the System.Decimal value (`QuantityEncoding.asSystemText`; the stored-quantity shortcut in
`toQuantity` is removed), and an out-of-range quantity neither converts nor reports that it can
(`QuantityValue.isSystemQuantity` reads the decoded value). A range boundary beyond the
System.Decimal bounds nothing; `FhirPathNumber`'s magnitude is computed from the digits, not a
double's logarithm; a search value with a negative scale is made plain. New tests: conversion and
out-of-range cases in `AnnotatedTypesParityTest`; edge, integer-path, rounding-boundary and repeating
decimal cases in `SearchValueSystemDecimalTest`; two boundary rows in `FhirPathNumberTest`.

**Approved (owner, 2026-11-12).** `QuantityCollectionTest` (both layouts), case 13,
`value.ofType(Quantity).toQuantity('g').value`: o5, a stored `0.0000002 kg`, gives `0.000000`
rather than `0.000200`, because the conversion takes its System.Decimal value, 0 kg.

**Test runs.** `mvn verify -pl library-api,legacy-export -am -Dmaven.test.failure.ignore=true` at
`f63e825b3b`: utilities 40, fhir-schema 100, encoders 620, io 797 (1 skipped), legacy-export 38,
terminology 926, fhirpath 8128 (888 skipped) with the two failures above and no others, library-api
380.

With that expectation changed, `mvn verify -pl library-api,legacy-export -am` at `e53fbaf27f` was all
green: utilities 40, fhir-schema 100, encoders 620, io 797 (1 skipped), legacy-export 38, terminology
926, fhirpath 8128 (888 skipped), library-api 380.
