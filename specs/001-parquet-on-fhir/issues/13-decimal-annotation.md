# 13 — Decimal numeric annotation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-021, FR-022, FR-023. Old tasks: T050a, T064, T086a, T095a.

**What to build:** A decimal stored as text carries a numeric annotation beside it, and the engine uses it as a fast path for comparison, arithmetic, ordering and decoding. The engine chooses the fast path from the schema, never per row. The lexical path stays the fallback and stays correct.

**Where the kind lives (decision 135).** The numeric kind is defined once, in the kinds package beside `fhir-schema`'s annotation API, and the engine's own computation of a decimal's value moves onto it. It is stored by the annotation step of ticket 11, not by the reader.

**Placement (decision 135, part 4).** The numeric annotation is `_<element>.__numeric`, a field of the decimal's metadata group, so the group is stored for it before ticket 17 stores ids and extensions. The engine reads it by pairing the element with `_x`, naming only the annotation fields it needs, so that the read prunes (`evidence/annotation-pruning.md`).

**The annotation's type (owner, 2026-10-26).** `__numeric` takes the engine's System.Decimal type, `DecimalCollection.DECIMAL_TYPE` (today `DECIMAL(32,6)`; the constant has since become `DecimalCollection.getDecimalType()`, which returns `SystemDecimal.TYPE`), named once rather than restated, so that reading the annotation gives exactly what decoding the text gives and the results T086 pins do not change. System.Decimal's type is defined once, in a module the kind in `fhir-schema` can use, and `DecimalCustomCoder`, the legacy layout's encoder, keeps its own. Ticket 41 then switches System.Decimal to `DECIMAL(38,6)`, the specification's type for `numeric`, so this ticket does not change it. Until FHIR and System primitives are separate types, every decimal result in the engine is a System.Decimal, so the engine reads the value from `__numeric` where the schema carries it and decodes the text otherwise. The text stays the stored value: egress writes it, and the annotation is only the engine's fast path to the number. Once FHIR and System types are separated, a FHIR.decimal may be returned from the stored text, and `__numeric` stays the fast path for its conversion to System.Decimal. That pays off fully only once the text preserves the source's lexical form (OQ-1, ticket 16).

**Blocked by:** 11

**Status:** done

- [x] A test shows a decimal carries the annotation beside its stored text, at the agreed type and position (T050a).
- [x] Decimal comparison, arithmetic and ordering with the annotation give results equal to the computed path T086 pins (T086a).
- [x] Decoding takes the annotation where the schema carries it (T095a). With the annotation disabled, the suite passes on the lexical path.

## Comments

### Implementation (branch issue/2832_13_decimal-annotation)

**What was built, and where.**

- **System.Decimal's type, defined once (owner, 2026-10-26).** `utilities`' `au.csiro.pathling.utilities.SystemDecimal` holds `DECIMAL(32,6)`, with the reasoning that `DecimalCustomCoder` gave for it. `DecimalCollection.getDecimalType()` returns it and the kind uses it directly, as do `QuantityEncoding.decodeValue` and `encodeNumeric` through the kind's `decode`, so ticket 41 changes one number. It is in `utilities` because terminology needs it too: `PropertyUdf` gives a decimal property value the type, and the engine computes with that value, so it now takes `SystemDecimal.TYPE` rather than `DecimalCustomCoder.decimalType()`. `DecimalCustomCoder` is left as it is, for the previous layout's data. Its one other use, `FlexiDecimal.toDecimal`'s return type in `encoders`, has no caller outside tests and is left on it.
- **The kind.** `kinds/DecimalNumericKind`, on the numeric switch, carried in `__numeric`. `compute` is `struct(try_cast(text as DECIMAL(32,6)) as __numeric)`, and its static `decode` is the engine's own decoding of a decimal, which the decimal traversal and `QuantityEncoding.decodeValue` and `encodeNumeric` call, so the annotation and the lexical path are one expression and cannot disagree. The kind's `read` decodes a resolved annotation to `SystemDecimal.TYPE`, whatever type it was stored at (see the re-review fixes). It is registered in `AnnotationRegistry.builtIn()`, so `encode` stores it through ticket 11's step, which is unchanged.
- **The engine's fast path (T095a).** `DefaultRepresentation.traverse` and `ResourceRepresentation.traverse` (root decimals such as `ChargeItem.factorOverride`) decode a decimal by pairing `x` with `_x` and resolving `_x.__numeric` through `ResolveOrElse`, so the choice is made once from the schema. A singular decimal reads the annotation alone, so the lexical plan is the same `try_cast` as before; under a repeating parent the parents without a value are left out first, so a text that does not decode still gives a null item, as before. A repeating decimal is paired with its annotations by position (`traverseAnnotated` nested, `pairWithAnnotation` at the root). The read prunes to `_x:struct<__numeric:decimal(32,6)>`.
- **Docs.** `contracts/storage-layout.md` (a decimal annotation section and the type deviation), `data-model.md`, and the annotation docs in `PathlingContext` (Java), `context.py` and `context.R`/`encoding.R`, which now say two kinds are stored.

**Tests.**

- T050a: `io/.../DecimalTransformTest` (7): the annotation is the last field of `_x` at `SystemDecimal.TYPE`, at the root, in a structure, under a repeating parent and in a quantity's `_value`; it is what decoding gives (`0.1234567` gives `0.123457`, the text keeping every digit); a value with more than 26 integer digits is annotated with null; the switch leaves it and the group out; egress writes the text without it.
- T086a and the disabled case: `DecimalCollectionTest` runs T086's 24 cases again over data annotated by default and over data annotated with the numeric annotation off, with the same expected values. A separate fixture puts a decimal beyond the range of the type under a repeating parent, and shows it gives the same items, a null among them, read, annotated and with the annotation off.
- T095a: `DecimalCollectionTest.decodingTakesTheAnnotationWhereTheSchemaCarriesIt` shows for a root, nested and repeating-parent decimal that the annotated plan reads `__numeric` and does not cast the text, that the scan reads only `__numeric` from the group, and that the unannotated and disabled plans cast the text and do not read the group.
- No existing expected value changed. The rest of the engine suite runs over the annotation-free fixtures (`AnnotationFreeSuiteTest`), so it is the lexical path that passes there.

**Test runs.** `mvn clean install -pl library-runtime -am`, all green: utilities 45, fhir-schema 49, encoders 595, io 771 (1 skipped), terminology 538, fhirpath 7478 (924 skipped, the exclusion baseline), library-api 333. `mvn clean install -pl lib/python,lib/R` with an empty Ivy home of its own through `SPARK_CONF_DIR`: Python 573 passed, R 224 passed, the retrieved `library-runtime` jar matching `library-runtime/target` by SHA-1.

**Left open.**

1. **A quantity's value does not take the fast path.** `valueQuantity._value.__numeric` is stored, but the System.Quantity's fixed shape (FR-059) leaves out `_value`, so `QuantityValue` and `QuantityMatcher` decode the text. Carrying the annotation would amend FR-059.
2. **The specification's type differs.** The upstream Parquet on FHIR README gives `numeric` as `DECIMAL(38,6)`, a sibling `__x_numeric`. The placement is ticket 35's; the type is the owner's decision here, and ticket 41 moves to `DECIMAL(38,6)`.
3. **The singular paths take a stored annotation without testing the value**, relying on the step storing null wherever the text is null. That holds for Pathling's own data under `trust` (ticket 31).


### Owner's decisions on the implementation (2026-10-26)

1. **A quantity's value does not take the fast path here.** Carrying `_value.__numeric` into the engine's System.Quantity shape, amending FR-059, is folded into ticket 41.
2. **The specification's placement and type.** No action beyond the storage-layout note: ticket 35 proposes the placement upstream, and ticket 41 switches the type to the specification's `DECIMAL(38,6)`.
3. **The plan for a decimal that itself repeats** stays as it is. It is a candidate for the performance follow-up that ticket 26 raises, since no R4 element is such a decimal and results are unchanged.

### Review fixes (2026-10-26)

- **Finding 3, fixed.** A stored `__numeric` is decoded to `SystemDecimal.TYPE` wherever the engine reads it: `DefaultRepresentation.numericValue` (a singular decimal, nested or at the root) and `numericValues` (a decimal that repeats, nested or at the root). Both go through `DecimalNumericKind.decode`, the safe cast the text takes, so an annotation written at another type is decoded to System.Decimal and one already at it is left as it is. `DecimalCollectionTest.anAnnotationStoredAtAnotherTypeIsDecodedToSystemDecimal` stores the annotation at `decimal(38,8)` for a nested, a repeating-parent and a root decimal, and shows the result is System.Decimal with the values T086 pins. The T095a plan tests still hold.
- **Finding 7, fixed after the owner's decision.** No main path reached the DECIMAL branch of `DefaultRepresentation.decodeField`; only ticket 11's `AnnotatedTraversalTest.decodesEachValueAndComputesItsAnnotationFromTheStoredText` did, by passing `decimal` to the public `traverseAnnotated`. That test is retargeted to `base64Binary`, the other type `decodeField` decodes, with the owner's approval as a retargeting rather than a changed result for the same input. The branch and `DecimalCollection.decode`, its only caller, are removed. `traverseAnnotated` keeps its `fhirType` parameter, which ticket 14 may use.
- **Finding 8, fixed.** `QuantityEncoding.encodeNumeric` decodes through `DecimalNumericKind.decode`.
- **Finding 10, fixed.** `DecimalCollection.DECIMAL_TYPE` and `DecimalNumericKind.NUMERIC_TYPE` are removed. `DecimalCollection.getDecimalType()` returns `SystemDecimal.TYPE`; the kind and its Javadoc name `SystemDecimal.TYPE`, and `YamlSupport` uses `getDecimalType()`.
- **Findings 1, 2 and 4** (data mixing annotated and unannotated files) are by design under FR-023, and go to ticket 38.
- **Findings 5 and 6** (the text column read under a repeating parent, and finding 6) go to ticket 26's performance follow-up.
- **Finding 9** (separate root and nested decimal traversals): no action.

`mvn clean install -pl library-runtime -am`, all green: utilities 45, fhir-schema 49, encoders 595, io 771 (1 skipped), terminology 538, fhirpath 7481 (924 skipped), library-api 333. Python and R were not run, since neither their code nor their docs changed. After the finding 7 fix the same run gave the same counts.

### Re-review fixes (2026-10-26)

- **Findings 1 and 9, fixed: the annotation is decoded where it is resolved.** `AnnotationKind` gains `read(resolved)`, identity by default, which takes an annotation as the engine reads it. `DecimalNumericKind.read` is its `decode`, to `SystemDecimal.TYPE`. Both of the engine's resolution sites apply it: `DefaultRepresentation.resolveAnnotation` for one value (`kind.read(kind.apply(...))`, used by `numericValue` and by `pairWithAnnotation`'s singular branch) and `pairWithAnnotation`'s repeating branch, at each annotation it zips out of `resolveOrElse` over the groups. So every reader of the kind's annotation, `traverseAnnotated` and ticket 41's quantity reader included, gets `SystemDecimal.TYPE`, and `numericValue` and `numericValues` no longer decode. The hook is applied to the resolved value rather than to the stored branch alone: `ResolveOrElse` takes no function of the stored field, and adding one to a Catalyst expression in `encoders` would widen ticket 11's resolver for one cast. On unannotated data the read is a cast of the computed annotation to its own type, which Spark's optimiser removes, so the computed branch has one cast; the T095a plan test now asserts there is no `try_cast(try_cast(`. The annotation step does not apply the hook, so *fill* keeps a stored annotation as it was found.
- **Finding 2, fixed.** `traverseAnnotated` refuses a `decimal` `fhirType` with an `IllegalArgumentException` saying that a decimal's value is its numeric annotation, which `traverse` reads. `AnnotatedTraversalTest.refusesToPairADecimal` shows it.
- **Findings 3 and 4, fixed.** `fhirpath/.../column/StoredNumericAnnotationTest` (8) stores `__numeric` at `decimal(38,8)` and at `decimal(38,0)`, for a singular and a repeating decimal, nested (`DefaultRepresentation`) and at the root (`ResourceRepresentation`'s singular and array branches), with synthetic data since no R4 element is a repeating decimal. Each result is `SystemDecimal.TYPE`, or an array of it. `0.12345678` stored at `decimal(38,8)` reads as `0.123457`, and `1e30` stored at `decimal(38,0)` reads as null, where a plain cast would fail.
- **Finding 5, fixed.** The retargeted case is `AnnotatedTraversalTest.decodesEachValueByItsTypeAndComputesItsAnnotationFromTheStoredValue`. It feeds `base64Binary` as it is stored, binary (`X'414243'`), and asserts the value (`QUJD`, base64 without padding) and the annotation (`ABC~`) separately.
- **Findings 6 and 7, fixed.** `QuantityEncoding.decodeValue`'s Javadoc points at `DecimalNumericKind#decode`. The Javadocs of `numericValue`, `numericValues`, `decodeValue`, `encodeNumeric`, the text-only value decode beside them, `DecimalCollection.withSharedSqlType` and `AbsentElementTypes` link `SystemDecimal#TYPE` rather than naming `DECIMAL(32,6)`.
- **Finding 8, fixed.** The implementation notes above describe the current design, and the owner's paragraph on the type notes that the constant has since become `getDecimalType()` / `SystemDecimal.TYPE`.

No existing test's expected result changed for the same input.


`mvn clean install -pl library-runtime -am`, all green: utilities 45, fhir-schema 49, encoders 595, io 771 (1 skipped), terminology 538, fhirpath 7490 (924 skipped), library-api 333. Python and R were not run, since neither their code nor their docs changed.
