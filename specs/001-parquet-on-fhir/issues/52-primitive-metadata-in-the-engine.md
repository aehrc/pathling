# 52 — Primitive metadata in the engine (US6, #437)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-034, US6. Old tasks: T120–T124. Decisions 135 (part 4), 141, 142 and 152. Split from ticket 17 (Primitive metadata in the stored layout; owner, 2026-11-12).

**What to build:** The engine pairs a primitive's values with its metadata group by position and reads its ids and extensions, behind a switch.

- **Pairing.** Ticket 11 (Annotation registry, per-annotation toggles and SchemaConfiguration) builds the pairing of `x` with `_x` for annotation fields, and ticket 51 (Emission shapes, level tests and the by-example tests) builds its emission shape for repeating primitives. This ticket extends it to `id` and `extension`, for singular and repeating elements alike. It names only the fields an expression needs, so that the read prunes. Nothing tests whether `_x` exists on its own.
- **Navigation.** Navigation to the id and extensions goes through sibling resolution (T124), and both yield empty where the source carried neither (T120–T123).
- **The switch (decision 141, D14; owner, 2026-11-10).** A configuration option, off by default, puts a primitive's `id` and `extension` into its engine element as content fields beside `value` (decision 152). With it off, `x.id` and `x.extension(…)` on a primitive are empty, with no error. It is a separate setting from the ingest option of ticket 17 (owner, 2026-11-12), so that data stored with the group can be read without it.
- **Conversion to System and a System merge (decision 142, D8 and D9; moved from ticket 43, owner, 2026-11-11).** With the switch on, the conversion to System drops `id`, `extension` and any other content the System type lacks, and a merge whose result is a System value drops each operand's `id` and `extension`, while a merge of FHIR values unions them. The rules are as decision 153 now states them, which changes what a converted System value is made from. They are not restated here.
- **Equality.** Primitive `id` and `extension` in comparisons stay as decision 151 settles, until the equality review of ticket 26.

**Blocked by:** 17, for the stored group; 51, for the pairing's emission shape; and 50 (Decimals and System.Quantity in the engine), for decision 153's conversion rules.

**Status:** ready-for-agent

- [x] With the switch on, `x.id` and `x.extension(…)` return the stored values for a singular and a repeating primitive, and empty where the source carried neither; with it off they are empty with no error. A test shows each.
- [x] The read names only the fields the expression needs, shown by a plan test. With the switch on, the test of whether a primitive without a value is an element reads its whole `extension` wherever the primitive is read; the owner accepted that cost (2026-11-13, review finding 3), and testing through `extension.url` is a candidate in ticket 26.
- [x] With the switch on, the conversion to System drops `id`, `extension` and any other content the System type lacks, a merge whose result is a System value drops `id` and `extension`, and a merge of FHIR values unions them; a test shows each, over a primitive that carries an extension.

## Comments

### Owner decisions before dispatch, 2026-11-13

1. **The element form.** With the switch on, every FHIR primitive is `struct(value, id, extension,
   <annotation fields>)`, flat, as design §3.1 of `annotations-by-example.md` shows, and every
   primitive collection reads its value from `value`.
2. **The switch.** A new boolean on `FhirpathConfiguration`, off by default, wired through
   `PathlingContext`'s existing configuration only. Its Python, R and server exposure is a
   follow-up.
3. **Plan tests.** They assert a pruned read where Spark 4.0.2 can prune (a primitive outside a
   lambda, with `id` and `extension` unread), and the shape only elsewhere, as ticket 51 did.
   _Amended (owner, 2026-11-13, review finding 3):_ once a primitive without a value but with
   metadata is an element, its `id` and whole `extension` are read wherever it is read, singular or
   repeating, so the plan test asserts that only named fields of the group are read. The owner
   accepted the full read of `extension`; testing through `extension.url` instead is a candidate
   in ticket 26.

### Built, 2026-11-13 (branch issue/2832_52_primitive-metadata-engine)

**The switch.** `FhirpathConfiguration.readPrimitiveMetadata`, off by default, ~~mirrored on
`QueryConfiguration` so that `FhirViewExecutor` and `PathlingContext.evaluateFhirPath` pass it on~~
(moved off `QueryConfiguration`, which the server binds, to `PathlingContext.Builder`; see below).
`DatasetEvaluatorBuilder` gained `withConfiguration`. No Python, R or server surface.

**How it travels.** With the switch on, `SingleResourceEvaluatorBuilder` wraps its resolver
(`PrimitiveMetadataResolver`), so that every resource is resolved under definitions that wrap each
definition reached from them (`fhirpath.definition`: `PrimitiveMetadata` and three decorators). A
wrapped primitive's definition has the children `id` (a plain string) and `extension` (wrapped in
turn), taken from the Element type (`FhirDefinitionContext.findElementChild`). The switch therefore
travels with a collection wherever its definition does, through `where`, `first`, `copyWith`,
lambdas and FHIR merges, and is absent from literals and function results.
`Collection.holdsPrimitiveMetadata()` tests for it.

**The element.** Traversal reads every primitive, at the root and under a parent, as
`struct(value, id, extension, <annotation fields>)`, each read from `_x` by name where the schema
has it (`StoredPrimitive`, and `SystemDateTime` and `StoredDecimal` with metadata fields), paired by
position for a repeating primitive, with the existing guards. A primitive without a value but with
an id or extensions is an element without a value, and one with neither is no element (owner,
2026-11-13, below). Navigation to `id` and `extension` is an ordinary read of the element.

**Conversion (D8).** `toEngineForm()` converts such a primitive to its System value: its value, or
for a date `SystemDateTime.withoutMetadata` (value and stored bounds), or for a decimal its number.
It is taken by comparison and arithmetic operands (`CombiningLogic.unifyShapes`), function
parameters typed by a primitive collection (`FunctionParameterResolver`), `castAs`, Boolean and
string coercion, `convertsTo*`, the indexer, a unit argument, `memberOf` on a code, the URL test of
`extension()`, reference keys, a view's `where`, and output (`getOutputColumn`, which
`Materializable.toExternalValue` now reads). A date compared is not converted: its comparator reads
only its text and bounds, and converting it lost the filter pushed down on a stored bound.

**Merges (D9) and equality (decision 151).** A merge of FHIR primitives keeps them and unions `id`
and `extension` by name (`CombiningLogic.mergeStructureOf`, a flat structure whose `extension` is
ordered by the Extension definition); a merge with a System value converts first. String, integer,
boolean and time compare and deduplicate by value (`StoredPrimitiveComparator`); dates and decimals
already did. The first occurrence is kept with its own id and extensions.

**Tests.** `PrimitiveMetadataTest` (50): navigation with the switch on and off, singular and
repeating, under a repeating parent, with id only, extensions only, a group without its value and
positional nulls, for a code, a date, a boolean, a string and a decimal; data stored without the
groups; values the same on and off and over annotated and unannotated data; the §3.1 switch-on
examples; conversion and both merge directions over primitives carrying extensions; view output;
plans. `AnnotationsByExampleTest`'s javadoc now points at it. No other test changed.

**Pruning on Spark 4.0.2.** ~~A singular primitive outside a lambda, output or compared, reads no
`id` or `extension` (asserted from the scan's read schema).~~ (Superseded by the owner's decision 1
below: a singular primitive reads both wherever it is read.) A repeating primitive reads them
whenever it is read, because the pairing filters through a lambda; only its shape is asserted.
Design §4.1's switch table is amended to say so.

**Verification run with the switch forced on (not committed).** Every remaining failure is one of:
search (`FhirSearchExecutorTest`, `SearchColumnBuilderTest`, `SearchValueSystemDecimalTest`,
`SelfJoinAmbiguityTest`, `JoinConditionTest`), which reads evaluated primitives as bare values and
has no switch; tests that read the engine's raw form through `toIdValueDataset` or a plan shape
(`TraceFunctionTest`, `DatasetEvaluatorTest`, `SingleResourceFhirPathTest`,
`ShapeReconciliationTest`, `ComparisonKeyTest`, `DateRangeComparisonTest`, `StoredQuantityTest`,
`QuantityExtensionThroughProjectionTest`, `IndexedQuantityExtensionTest`, `AnnotationsByExampleTest`,
`EmissionShapesTest`); and fhirpath-js cases where a given name carries an id but no value, which
the owner's decision below resolved.

### Owner decisions after the build, 2026-11-13

1. **With the switch on, a primitive slot with no value but with an id or extensions is an element**,
   a structure with a null value and its id and extensions where the schema has them, as fhirpath.js takes it. A slot with
   neither is still not an element. Its System value is empty, so it converts to nothing, compares
   to nothing and is output as nothing; it is counted, indexed and navigated to. The brief's rule
   that such a slot is no element is withdrawn.
2. **With the switch off, the engine matches `main`.** On `main` a repeating primitive under a
   repeating parent keeps its positions without a value as null items, because null items are
   removed before the parents' arrays are flattened, while at the root and under a singular parent
   they are removed.
3. Follow-up: ticket 54 (Primitive metadata in the conformance baselines and the default build).

### Built after the owner's decisions, 2026-11-13

- **Value-less elements (1).** `DefaultRepresentation.storedValues` pairs without filtering
  (`SystemDateTime.paired`, `StoredDecimal.paired`, `StoredPrimitive.paired`) and then keeps a
  position where its value, or, with the switch on, its `id` or `extension`, is present. Conversion
  to System leaves out the elements without a value. Deduplication keys a primitive by its value in
  a structure, so that the elements without a value share one key and are kept once, as fhirpath.js
  keeps them (`Patient.name.given.distinct().count()` is 3). The existence of a singular element
  now depends on its `id` and `extension`, so a singular primitive reads both wherever it is read:
  the plan test asserts that the read names only fields of the group, never the group, and no
  annotation that the expression does not use.
- **Switch off (2).** The branch already matched `main` for every primitive but a date or decimal,
  which tickets 14 and 50 read through their metadata group: those left out a repeating one's
  positions without a value under a repeating parent. They now keep them as null items, as `main`
  does. No protected expectation changed.
- **Verification.** fhirpath-js and fhirpath-ptl with the switch on and the YAML harness storing
  primitive metadata (local overrides, not committed): every case that is not excluded passes,
  including the eight `Patient.name.given` cases above, and 18 excluded cases pass, which ticket 54
  is to re-enable.

### Independent review, fixes, 2026-11-13

- **A choice of several FHIR types** (`extension('u').value.ofType(System.String)`) took its
  variants as held, with no definition, so the result held structures. Each variant is now taken as
  its System value before the variants are coalesced.
- **Dates, dateTimes and decimals without a value** were output and computed with as null items.
  Their output and conversion now leave them out, as every other primitive's does. A deduplication
  keeps one of them, for a decimal (`StoredDecimalComparator.withMetadata`) and a time
  (`StoredPrimitiveComparator`, whose equality takes two of them as equal) as for every other type.
- **The switch is off `QueryConfiguration`** (owner, review finding 4: no server surface). The
  server binds `QueryConfiguration` from its Spring configuration, so the library sets the switch
  through `PathlingContext.Builder.readPrimitiveMetadata`, which `PathlingContext` passes to
  `FhirViewExecutor` and `evaluateFhirPath`. Server exposure is part of ticket 54.
- Dead code removed: `SystemDateTime.fromStored` and `fromStoredArray`, `StoredDecimal.fromStored`
  and `fromStoredArray`; `AnnotationMemoisationTest` reads the date through `SystemDateTime.paired`.
- Tests added to `PrimitiveMetadataTest` for each of the above, and one that pins the switch off:
  a repeating date and decimal under a repeating parent keep a position without a value as a null
  item.
