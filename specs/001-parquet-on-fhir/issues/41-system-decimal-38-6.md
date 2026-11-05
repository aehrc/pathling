# 41 — Switch System.Decimal to DECIMAL(38,6)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002. The Parquet on FHIR specification's `numeric` annotation. Raised from ticket 13 (owner, 2026-10-26).

**What to build:** The engine's System.Decimal becomes `DECIMAL(38,6)`, the type the [Parquet on FHIR specification](https://github.com/aehrc/parquet-on-fhir) gives the `numeric` annotation. Ticket 13 gives `__numeric` System.Decimal's type, so after this ticket the stored annotation matches the specification, and the engine reads it without a conversion.

**Why it changes few results.** Spark computes `+`, `-`, `*` and `/` over `DECIMAL(32,6)` operands at `DECIMAL(38,6)` already, and the engine casts the result back to System.Decimal. Both types keep 6 fractional digits, so rounding is unchanged. What changes is the range: a value with 27 to 32 integer digits, null today, now has a value. For example, `1e14 * 1e14` gives `1e28` rather than null, and a stored `1e30` decodes rather than becoming null.

**What it touches.**

- **One definition.** System.Decimal's type is defined once, in the module ticket 13 put it in, and no longer borrows its precision from `DecimalCustomCoder`. `DecimalCustomCoder`, the legacy layout's encoder, stays `DECIMAL(32,6)`, because `legacy-export` and the layout detection read legacy data at that type.
- **Its users.** `DecimalCollection` (decoding, arithmetic, literals and the precision check in `toLiteral`), `QuantityEncoding` (a quantity's value), `ConversionLogic` (`toDecimal()`), `IntegerCollection` (integer to decimal), `FlexiDecimal.fromValue` and `terminology`'s `PropertyUdf` take System.Decimal's type rather than `DecimalCustomCoder`'s.
- **Output.** A decimal column in a view's output, in Java, Python and R, becomes `DECIMAL(38,6)`. That is a change users see, so it goes in the release notes, and ticket 40 records it in the shape of SQL output.
- **Comments and docs** that name `DECIMAL(32,6)` as the engine's type.

**A quantity's value takes the fast path too (owner, 2026-10-26).** Ticket 13 stores `_value.__numeric` on a quantity, but the engine's fixed System.Quantity shape (FR-059) has no `_value`, so `QuantityValue` and `QuantityMatcher` decode the stored text. Carry the numeric annotation into that shape, amending FR-059, so that reading a quantity's `value` and arithmetic on it take the annotation where the schema carries it, and decode the text otherwise. Comparison already takes `__canonical_exact` (ticket 12).

**An owner-approval gate.** Each existing test whose expected value changes is listed with its old and new values before it is changed. Known cases: `AnsiModeIndependenceTest` (`1e28`), `ColumnRepresentationSafeCastTest` (`1e30`) and `ProjectedColumnTypeTest` (the output type).

**A recorded deviation (owner, 2026-10-26).** FHIRPath requires a Decimal to provide at least the range (-10^28+1)/10^8 to (10^28-1)/10^8 with a step of 10^-8, so 8 fractional digits (`DECIMAL(28,8)`). `DECIMAL(38,6)` covers the range but keeps 6 fractional digits, as `DECIMAL(32,6)` does today. Record this as a known deviation in the specification. Whether to move to `DECIMAL(38,8)`, with an upstream proposal for the annotation's type, is a follow-up issue that ticket 26 raises.

**Blocked by:** 13

**Status:** done

- [x] System.Decimal is `DECIMAL(38,6)`, defined once, and `DecimalCustomCoder` stays `DECIMAL(32,6)`.
- [x] Every engine use of a decimal type takes System.Decimal's, and `__numeric` is stored at `DECIMAL(38,6)`.
- [x] A quantity's value is read from its numeric annotation where the schema carries it, with FR-059 amended, and gives the same results as decoding the text.
- [x] Each changed test expectation is approved by the owner before it is changed.
- [x] The output type change is documented, and the 8-digit deviation is recorded in the specification.

## Comments

### Implementation (branch issue/2832_41_system-decimal-38-6)

**What changed.**

- **System.Decimal is `DECIMAL(38,6)`.** `utilities`' `SystemDecimal` holds precision 38, scale 6, defined once. Every engine use already took it through ticket 13 (`DecimalCollection`, `QuantityEncoding`, `ConversionLogic`, `IntegerCollection`, `PropertyUdf`, `DecimalNumericKind`), except `FlexiDecimal.toDecimal`'s return type, which now takes it too. `DecimalCustomCoder` stays `DECIMAL(32,6)` for the legacy layout and `LayoutDetector`. Ingest stores `__numeric` at `DECIMAL(38,6)` through `DecimalNumericKind`, unchanged.
- **A quantity's value takes the annotation (FR-059 amended).** The System.Quantity gains `_value`, a struct of `__numeric` at System.Decimal's type, after `code` and before `__canonical_exact`, so the six element fields keep their positions (the engine reads the group by name). It is filled whatever produced the quantity: a stored quantity copies `_value.__numeric` where the schema carries it and decodes the text otherwise (`QuantityEncoding.numericValue`, through `DecimalNumericKind`'s `apply` and `read`); a literal and a UDF result (`toQuantity`, unit conversion) compute it in Java with Spark's `Decimal.changePrecision(38,6)`, the rule the safe cast applies; a promoted number carries the decoded number. `QuantityValue` (comparison by value) and `QuantityMatcher` (search) read `numericValue`, and `.value` on a System quantity reads the group through the existing decimal traversal. `QuantityEncoding.decodeValue(Column)` is removed. The group is cast to its type when built, because Spark cannot name a UDF over a quantity holding an uncast nested struct (`Cannot evaluate expression: __numeric`).
- **Spec and docs.** FR-035 amended to `DECIMAL(38,6)` with the 8-digit FHIRPath deviation recorded; FR-059 amended for `_value.__numeric`; `contracts/engine-semantics.md` (the cap, the System.Quantity table), `contracts/storage-layout.md` (the annotation now at the specification's type, only the placement deviates), `contracts/library-api.md` (behaviour change: decimal output is `DECIMAL(38,6)`; the System.Quantity fields), `data-model.md`, ticket 40 (a pointer), and the site's schema page (query-time precision).

**Tests.** New `fhirpath/.../QuantityNumericAnnotationTest` (30): over read, annotated and numeric-annotation-disabled data, quantity comparison by value, a stored quantity's value through a System quantity, literal and converted quantities' values (rounding and overflow), and value searches give the same results; and on annotated data the plan reads `__numeric` and does not cast the value text, while the unannotated plans do.

**Test runs.** `mvn verify -pl library-api,legacy-export -am`: utilities 45, fhir-schema 54, encoders 599, io 788 (1 skipped, 1 failing), legacy-export 38, terminology 538, fhirpath 7633 (924 skipped, 19 failing), library-api 342, run with the YAML harness change of item 9 applied; without it the 9 DSL methods of item 9 fail too. Every failure is an expectation awaiting approval (below). Python and R were not run.

**For owner approval (expectations not changed).**

1. `io/.../DecimalTransformTest.aValueBeyondTheRangeOfTheTypeIsAnnotatedWithNull`: `123456789012345678901234567` (27 integer digits) annotates as `123456789012345678901234567.000000`, not null. Needs a value beyond 32 integer digits to keep its purpose.
2. `fhirpath/.../AnsiModeIndependenceTest.decimalOverflowProducesEmptyUnderAnsiOn`: `1e14 * 1e14` gives `1e28`, not `[null]`. The parity tests `decimalMultiplicationOverflowIsIdenticalUnderBothSettings` (`1e14 * 1e14`) and `outOfRangeQuantityValueIsIdenticalUnderBothSettings` (`1e30`) still pass but no longer overflow; their inputs need retargeting (e.g. `1e17 * 1e17`, `1e33`) to test what their names say.
3. `fhirpath/.../ColumnRepresentationSafeCastTest`: passes, because it pins `DECIMAL(32,6)` itself; retargeting it to `SystemDecimal.TYPE` makes `1e30` give `1e30` rather than null.
4. `fhirpath/.../StoredNumericAnnotationTest` (4 cases, `aSingularDecimalNested`, `aSingularDecimalAtTheRoot`, `aRepeatingDecimalNested`, `aRepeatingDecimalAtTheRoot`, each the `decimal(38,0)` case): `1e30` stored at `decimal(38,0)` reads `1e30`, not null.
5. `fhirpath/.../DecimalCollectionTest.aDecimalBeyondTheRangeGivesTheSameItemsOnEveryPath`, `prediction.probability.ofType(decimal)[1]`: `r1=null` becomes `r1=1000000000000000000000000000.000000` (`1e27` is now in range) on every path; the fixture needs a value beyond 32 integer digits.
6. `fhirpath/.../DecimalCollectionTest.decodingTakesTheAnnotationWhereTheSchemaCarriesIt` (3 cases): the plan pattern `decimal\(32,6\)` and the read schema `struct<__numeric:decimal(32,6)>` become `decimal(38,6)`.
7. `fhirpath/.../SystemQuantityShapeTest.engineBuiltQuantityHasTheFixedShape` (10 producers): the fixed shape gains `_value: struct<__numeric: decimal(38,6)>` between `code` and `__canonical_exact`.
8. `fhirpath/.../ProjectedColumnTypeTest.declaredDecimalTypeStaysTextOnOutput`: no value change; a declared `decimal` view column is output as text (decision 76), so only its comment changed to `DECIMAL(38,6)`. A decimal column without a declared type is `DECIMAL(38,6)`.
9. **The DSL tests' expected System quantities** (9 methods, 54 cases): `AsOperatorDslTest.testAsOperator`; `CombiningFunctionsDslTest.testCombineQuantityAndCodingNoDedup`, `testUnionFunctionEquivalentToPipeAcrossTypeMatrix`, `testUnionFunctionTemporalAndQuantityAndCoding`; `CombiningOperatorsDslTest.testQuantityUnion`; `ConversionFunctionsDslTest.testToQuantity`, `testToQuantityWithUnit`; `ExistenceFunctionsDslTest.testIsDistinctAndDistinctTypeMatrix`; `TypeFunctionsDslTest.testAsFunction`. Each quantity result gains the `_value` slot, `null` expected and `[n.000000]` actual, e.g. `(11 'mg') as Quantity`: `[null, 11.000000, null, mg, http://unitsofmeasure.org, mg, null]` becomes `[…, mg, [11.000000]]`. The YAML files are unchanged; the expectation is built by `YamlSupport.YamlRepresenter.writeQuantity`, and the fix, left unapplied, is to write the group there after `code`: `gen.writeObjectFieldStart(QuantityEncoding.VALUE_GROUP_COLUMN); gen.writeNumberField(LayoutFields.NUMERIC_ANNOTATION, quantity.getValue()); gen.writeEndObject();`.
10. **Passing unedited, the asserted type or shape moved with the constant**: `DecimalTransformTest:112`, `StoredNumericAnnotationTest:164/170`, `DecimalCollectionTest:283` (`SystemDecimal.TYPE`, now `decimal(38,6)`); `ProjectionResultTest:139/156`, `QuantityTypesTest:168`, `QuantityEncodingTest:90/100`, `DefaultYamlTestExecutor:485` and `YamlSupport`'s `FHIR_TO_SQL` (`getDecimalType()` and `QuantityEncoding.dataType()`, now `decimal(38,6)` and the shape with `_value`).

No YAML conformance case or exclusion changed. No Python or R test asserts the decimal output type.

**Items for owner review.**

- FR-035 was a MUST that query-time precision stay as before this programme; it now names `DECIMAL(38,6)`, which the ticket implies but which changes a MUST.
- The `_value` group follows the elements rather than standing beside `value` as in storage.
- Data annotated by ticket 13's code stores `__numeric` at `DECIMAL(32,6)`, null for values of 27 to 32 integer digits that the text now decodes; under `trust` the engine takes the stored null. Nothing was released with it.
- Release notes: no file in the repository holds them; `contracts/library-api.md` records the behaviour change for the release.
- `FlexiDecimal.toDecimal` has no caller outside tests; it moved to System.Decimal's type rather than staying on the legacy coder's.

### Owner's decisions (2026-10-29)

1. **The gate is approved in full.** The overflow and boundary fixtures move above the new range, so each keeps its purpose: `DecimalTransformTest` annotates a 33-digit integer with null; `AnsiModeIndependenceTest` takes `1e17 * 1e17` for the overflow tests and `1e33` for the out-of-range quantity; `ColumnRepresentationSafeCastTest` casts to `SystemDecimal.TYPE` and takes `1e33`; `StoredNumericAnnotationTest` stores `1e33` at `decimal(38,0)`; `DecimalCollectionTest`'s out-of-range fixture is `1e33`. The `decimal(32,6)` plan and read-schema assertions become `decimal(38,6)`. `SystemQuantityShapeTest` pins the shape with `_value` and reads the elements by name. `YamlSupport.writeQuantity` writes the expected `_value.__numeric` beside `value`.
2. **`_value` stands right after `value`**, as the layout stores it: `id, value, _value, comparator, unit, system, code, __canonical_exact`. `QuantityEncoding`'s shape, its structure builder and every producer follow that order, and FR-059, `contracts/engine-semantics.md` and `contracts/library-api.md` give it. `data-model.md` does not give the order.
3. **The FR-035 rewrite is approved as written.**
4. **Data annotated by ticket 13's code at `DECIMAL(32,6)`:** no action. It was never released.
5. **`FlexiDecimal.toDecimal` stays on System.Decimal's type.**

`mvn verify -pl library-api,legacy-export -am`, all green: utilities 45, fhir-schema 54, encoders 599, io 788 (1 skipped), legacy-export 38, terminology 538, fhirpath 7633 (924 skipped, the exclusion baseline), library-api 342. Python and R were not run.
