# 42 — Memoise an annotation kind's shared computation

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-005, FR-022, FR-023, FR-059. Raised from ticket 14's review of the System date (owner, 2026-10-28). Settled by the annotations design, `design/annotations-in-the-engine.md` §4.6, §4.7 and §7, and decisions 145, 149, 150 and 152. Examples: `design/annotations-by-example.md`.

**What to build:** The ticket asked whether a kind's fields stay grouped so that one computation serves them, or are read one by one over a cached computation. Decision 145 settles it: each annotation is its own scalar computation over a memoised parse, so the computation does not decide the layout. This ticket builds that for dates and quantities. It first writes the baseline plan tests for itself and for ticket 43 (owner, 2026-11-10).

1. **Baseline plan tests, first (design §7).** Written against today's code and committed before anything else in this ticket or 43 changes, so that any change to what tickets 11 to 14 delivered shows as a change to these tests. They assert today's plan; a later change that alters it updates the test and says why.
   - An output-only date under a repeating parent, and the same after `where()`: whether the optimised plan computes the date's range.
   - A merge of different shapes: an annotated date with an unannotated one, and a stored quantity with a literal (`design/annotations-by-example.md` §5.1).
   - `birthdate=ge2000` and `where(birthDate >= @2000)` over annotated Parquet: the scan's pushed filters.
2. **A memoised date range.** `DateRangeKind`'s struct-returning `date_range` is replaced by two scalar UDFs, `date_start` and `date_end`, over one parse held in a least-recently-used cache per thread (design §4.6):
   - a cache of about 16 to 64 entries, not the last input only, because a plan can read the fields in separate passes; the size is an implementation detail (decision 145);
   - cached values are immutable, the UDFs stay deterministic, and a parse failure is handled in each call, never cached;
   - a traced input is still bound with `SqlFunctions.let`, because the cache saves the parse, not the evaluation of the input (#2594).

   `ColumnFunctions.resolveFieldsOrElse` is replaced by one `resolve_or_else` per field, and `ColumnBinding.bindOnce` is removed wherever it only shares the parse: in `SystemDateTime`, `TemporalComparator`, `DateMatcher` and `Annotator`. The System date carries `__start` and `__end` as flat fields beside `value` (decision 152). Making them lazy, so that they are not resolved where nothing reads them, is ticket 43.
3. **Flat quantity canonical fields.**
   - **Ingest** writes `__canonical_code` (string) and `__canonical_value_exact` (FlexiDecimal, scale never negative) flat inside the quantity, in place of the grouped `__canonical_exact` (FR-005, decision 149, `contracts/storage-layout.md`). This is done now, not after ticket 46 (owner, 2026-11-10); if the specification's owner decides otherwise, the layout changes again then.
   - `QuantityCanonicalKind` computes both fields over one memoised canonicalisation, as the date range does. `QuantityEncoding.compareCanonical` reads plain fields.
   - **System.Quantity** keeps its shape, with the two canonical annotations flat in place of `__canonical_exact` and always filled; `SystemQuantityShapeTest` pins that interim shape. Decision 150's shape, with lazy canonical annotations, and the return of the four content fields only, moved to ticket 43, which builds the `ResolveOrNone` and merge rule they need (owner, 2026-11-10).
4. **Evidence.** `evidence/ticket-42-memoisation.md` records, on Spark 4.0.2:
   - the parse runs once per value, singular and repeating, inside a lambda, and when the fields are read in separate passes;
   - a traced input fires once;
   - throughput against today's grouping with `bindOnce`, on the existing benchmark. Any slowdown is brought to the owner before the ticket closes.

**Not in this ticket:**

- Grouped annotations (`__range{start, end}`, `__canonical{code, value}`) stay parked as design §6 question 1 (owner, 2026-11-10). The flat fields are built.
- The per-thread cache on Photon and Databricks is not checked here (owner, 2026-11-10). It goes to the profiling follow-up, #2889 (ticket 26). A miss costs a parse, never a wrong result.
- The lazy accessors, conversion, merges, comparison keys and emission shapes are ticket 43, and so is decision 150's System.Quantity shape (owner, 2026-11-10).

**Blocked by:** None. Ticket 14 is done.

**Status:** done

- [x] The baseline plan tests are committed against today's code before any other change of this ticket or 43.
- [x] `date_start` and `date_end` replace `date_range`. No struct-returning range computation and no `resolveFieldsOrElse` remain, and `bindOnce` remains only where it protects something other than a shared parse.
- [x] Ingest writes `__canonical_code` and `__canonical_value_exact`, and nothing writes or reads `__canonical_exact`.
- [x] Ingest tests show the flat fields written at every level: a quantity at the root (`Observation.valueQuantity`), under a repeating parent (`Observation.component.valueQuantity`), inside an extension (an extension's `valueQuantity`), and a repeating date under a repeating parent; each over a fresh schema and over data already carrying the fields.
- ~~System.Quantity has the shape of decision 150, and a returned one has its four content fields only; `SystemQuantityShapeTest` asserts both.~~ Moved to ticket 43 (owner, 2026-11-10).
- [x] `evidence/ticket-42-memoisation.md` shows the three measurements above, and any slowdown has gone to the owner.
- [x] Results are unchanged: the engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`).

## Comments

### Implementation, 2026-11-10 (branch issue/2832_42_memoise-annotations)

**Built.**

- **Baseline plan tests, first** (`3e5c4829a4`, `fhirpath/.../collection/AnnotationPlanBaselineTest`, 8 tests), written and passing against `d4ee59b73e` before anything else changed. Today's baseline: an output-only `name.period.start`, and the same after `where()`, computes the range over unannotated data and reads the stored `_start` group over annotated data; `birthDate | @2000` and `value.ofType(Quantity) | 5 'kg'` give the fixed System shapes on both sides, over annotated and unannotated data alike; `birthdate=ge2000` and `where(birthDate >= @2000)` push no filter on `__start` or `__end` to an annotated scan. Two of the tests changed later in this ticket, each with a comment saying why (below).
- **Memoised date range** (`c3765b42d6`). `DateRangeKind` has two scalar UDFs, `date_start` and `date_end`, and their `_or_null` forms for conversion, over one `FhirDateTime` parse held in `PerThreadLruCache`, a `ThreadLocal` least-recently-used cache of 64 entries (`fhir-schema`, public Spark API only). Cached values are immutable, a failed parse is never cached, and the UDFs stay deterministic. `AnnotationKind.compute` now computes one named field, so a kind no longer returns a structure. `ResolveFieldsOrElse` and `ColumnFunctions.resolveFieldsOrElse` are removed, with their test. The System date is `struct(value, __start, __end)`, flat (decision 152), each bound resolved on its own with `resolve_or_else`; a repeating date reads each bound in its own pass. `bindOnce` is gone from `SystemDateTime`, `DateMatcher` and `Annotator`; in `TemporalComparator` and `DateMatcher` the operand is bound with `SqlFunctions.let` instead, so a traced operand still fires once.
- **Flat quantity canonical fields** (`7bc7804cd7`). Ingest writes `__canonical_code` (string) and `__canonical_value_exact` (FlexiDecimal, scale never negative) flat and last in the quantity, in place of `__canonical_exact`. `QuantityCanonicalKind` computes each with its own UDF, `quantity_canonical_code` and `quantity_canonical_value_exact`, over one canonicalisation memoised the same way, keyed by value, system and code. `QuantityEncoding.compareCanonical` reads the two plain fields of each side. The System.Quantity carries the two flat fields in place of the grouped one and is otherwise as it was (see the open item below). `data-model.md` and ADR 0003 are updated; `contracts/storage-layout.md` already described the flat fields.
- **Evidence** (`evidence/ticket-42-memoisation.md`, `scripts/ticket-42/`, `AnnotationMemoisationTest`, two new cases in `ComparisonTraceTest`).

**Ingest at every level** (`io/.../FlatAnnotationLevelsTest`, 8 tests): `valueQuantity`, `component[].valueQuantity`, an extension's `valueQuantity`, and `dosageInstruction[].timing.event[]`, each annotated over a fresh schema, where the plan computes the fields, and over data already carrying them, where the plan computes nothing and the schema and values are unchanged.

**Awaiting the owner.**

1. **Gate 1, changed expectations.** In the commits labelled as awaiting approval (`1eb7cee138`, `f55ca93429`), and the adaptations inside the feature commits:
   - `DateRangeComparisonTest.theComputedRangeTakesOneParse` (2 cases): was one `date_range(` in the optimised plan and none over the literal; now five bound computations, `date_start` three times and `date_end` twice, none over the literal, and at run time no more parses than rows. Each bound is computed where it is read, and parse sharing is now a property of the run, not of the plan.
   - `DateRangeComparisonTest.parses()` and `literalParses()`: count `date_(start|end)(` instead of `date_range(`. The assertions that use them (`comparisonTakesTheStoredRangeWhereTheSchemaCarriesIt`, 8 cases; `dateSearch`, 9 cases) are unchanged in form: fewer computations annotated than computed, none annotated, some computed.
   - `io DateRangeAnnotationTest.bothBoundsComeFromOneParse`: was exactly one `date_range(` per annotated date; now exactly one `date_start(` and one `date_end(`, and no more parses at run time than dates.
   - `encoders ResolveFieldsOrElseTest`: deleted with `ResolveFieldsOrElse`, which the ticket removes.
   - `AnnotationPlanBaselineTest.aStoredDateMergedWithALiteralCarriesARangeOnBothSides`: `struct<value, range: struct<__start, __end>>` became `struct<value, __start, __end>`; values unchanged.
   - `AnnotationPlanBaselineTest.aStoredQuantityMergedWithALiteralIsAFixedShapeSystemQuantity`: the last field `__canonical_exact: struct<value: struct<value, scale>, code>` became `__canonical_code: string, __canonical_value_exact: struct<value, scale>`; the row `…, [[72500,0],g]` became `…, g, [72500,0]`.
   - `SystemQuantityShapeTest` (42 cases): the fixed shape ends in the two flat fields instead of `__canonical_exact`. This pins an interim shape that no decision names; see item 3.
   - `DefinitionCanonicalStructureTest` (Quantity field order), `QuantityCanonicalAnnotationTest`, `AnnotatorTest` (its stub canonical kind now has two fields, and `__unknown` follows both), `DecimalTransformTest.theValueOfAQuantityCarriesTheAnnotation`, `AnnotationFreeSuiteTest` (slot lists), `QuantityCollectionTest` (the plan reads `__canonical_value_exact`), `EvaluateFhirPathTest.quantityResultCarriesNoCanonicalAnnotation`: the field name, or the last field, is now the flat pair; no value changed. `SingleInstanceEvaluatorTest` uses the name only as a synthetic `__` field to strip, renamed.
   - API adaptations with no assertion changed: `QuantityCanonicalKindTest` (`toExactRow` became `toExact` and `toValueExactRow`; a new case for a scale that is never negative), `NumericQuantityCanonicalTest`, `QuantityTypesTest`, `DefaultYamlTestExecutor` (the quantity is compared up to `__canonical_code`), `SystemDateTimeCutoverTest`.

   `f55ca93429` cannot be reverted on its own: the tests it changes name constants that no longer exist.
2. **Gate 2, slowdowns.** On the engine probe, over unannotated data in three alternated rounds: annotating quantities is slower in every round (+28%, +6%, +15%); a date that is only output under a repeating parent is slower in two rounds (−2%, +4%, +7%), within the spread between rounds of the same jar (up to 11%) but on the slow side. A quantity comparison and the annotation of dates are unchanged within that spread; date comparisons inside a lambda (−28% to −36%) and date searches (−20% to −25%) are faster in every round. On the existing JMH benchmark, `encode` is as fast or faster for every resource type in both orders, and `viewExecution` shows no slowdown beyond JMH's error. The cause of the quantity slowdown is not measured. Nothing was done about either.
3. **Gate 3, System.Quantity (box 5 not met).** Decision 150's shape, with the canonical fields present only where they are available without computing, gives System.Quantity more than one Spark type, and the engine assumes one: `FhirPathType.QUANTITY` has one SQL type, the conversion UDFs return `QuantityEncoding.dataType()`, `toUnit` and `ConversionLogic` cast nulls to it, and `CombiningLogic` combines System operands without reconciling them, so `value.ofType(Quantity) | 5 'kg'` over unannotated data would combine two struct types. Only `ResolveOrNone` and the intersecting merge rule make that work, and both are ticket 43's. Filling an absent field with null is ruled out by design §4.4. Options: (a) a fixed shape `{value DECIMAL(38,6), unit, code, system, __canonical_code, __canonical_value_exact}`, always filled as today, with the canonical fields made lazy in 43; (b) bring `ResolveOrNone` and the merge rule into 42; (c) move the reshape, and the four-field return, to 43. Option (a) also changes results: a stored quantity with a comparator has no System form, so it would leave a union, and `toString()` and unit conversion of a stored value would work from `DECIMAL(38,6)` rather than its text. Meanwhile `contracts/engine-semantics.md` §Quantities describes decision 150's shape, which the code does not have yet.
4. **The time comparator keeps `bindOnce`.** `TemporalComparator.forTime()` still binds a structure of the two time bounds with `bindOnce`, because each bound is its own UDF, not memoised, and the comparison reads them several times: it saves computing each more than once, not a shared parse, which box 2 allows. Ticket 43's box says `TemporalComparator` contains no `bindOnce`.

**Also for the owner, not blocking.**

- `DateRangeKind.getParseCount`, `QuantityCanonicalKind.getCanonicalisationCount` and `PerThreadLruCache.getComputationCount` are public production methods that only tests and the evidence read.
- The JMH `viewExecution` benchmark fails as checked in, on the build before this ticket as after it: its Spark session writes timestamps as `INT96`, so the Delta sink refuses the range annotations. It was run with `-Dspark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`.
- A schema that carries one bound of a date, or one canonical field of a quantity, now reads that field and computes the other. Before, `resolveFieldsOrElse` computed both unless both were stored. Results are the same; the rule is FR-023's and design §4.7's.
- Data annotated before this ticket carries `__canonical_exact`, which the engine now treats as an unrecognised annotation, computing the canonical form. Nothing was released with it.
- The baseline pushdown tests assert only that no filter on `__start` or `__end` is pushed (and read an absent `PushedFilters` entry as empty), so they stay green until ticket 43 changes them to the positive form.

**Test runs.** `mvn verify -pl library-api,legacy-export -am`, all green: utilities 45, fhir-schema 61, encoders 605, io 796 (1 skipped), legacy-export 38, terminology 926, fhirpath 7934 (888 skipped), library-api 379. `AnnotationFreeSuiteTest` (15) passes within it. Python and R were not run. Box 6 is left open until the slowdown of item 2 is decided; box 5 is gate 3.

### Owner's decisions, 2026-11-10, and what followed

1. **Gate 1: every changed test expectation listed above is approved (owner, 2026-11-10).** The commits marked as awaiting approval, `1eb7cee138` and `f55ca93429`, stand as they are.
2. **Gate 3: decision 150's System.Quantity shape, and the return of the four content fields only, move to ticket 43 (owner, 2026-11-10),** which builds the `ResolveOrNone` and merge rule they depend on. Ticket 42 keeps today's shape with the two canonical annotations flat, which `SystemQuantityShapeTest` pins. The scope and box above are updated, ticket 43 takes the requirement and a box, and `contracts/engine-semantics.md` §Quantities says ticket 43 delivers the shape it describes and gives the interim one.
3. **The time comparator binds its operand with `SqlFunctions.let` (owner, 2026-11-10)** (`7a7505702c`). Its two bounds are computed wherever they are read, and a traced time fires once (`ComparisonTraceTest.temporalComparator_tracedTime*`). No `bindOnce` is left in the code, so `ColumnBinding` is removed.
4. **Gate 2: profile the quantity slowdown and fix it if a fix is cheap (owner, 2026-11-10).** The cause is the second function call over the same quantity: a second conversion of its structure and a cache lookup, about 80 ns a quantity. The cache now compares the last input before hashing it (`2d9da8b3e0`), which cuts that to about 55 ns. Re-measured in three rounds, annotating quantities is still slower by about 10% to 20% (+9%, +11%, +21%, the last round carrying an order effect), and an output-only date under a repeating parent by +4% to +9%; date comparisons and searches stay 15% to 32% faster. No cheap fix closes the rest. **Awaiting the owner:** accept it; pass the kind only the fields it reads (about 3%); or compute both canonical annotations at ingest from one call bound once per quantity, which groups the computation at ingest again. The output-only date's cost goes with ticket 43, which makes it compute nothing. Details in `evidence/ticket-42-memoisation.md`. Box 6 stays open until the owner decides.

**Test runs after these changes.** `mvn verify -pl library-api,legacy-export -am` at `2d9da8b3e0` plus documentation, all green: utilities 45, fhir-schema 62, encoders 605, io 796 (1 skipped), legacy-export 38, terminology 926, fhirpath 7936 (888 skipped), library-api 379. Python and R were not run. **Status** stays ready-for-agent while box 6 awaits the owner's decision on the remaining quantity slowdown.

### The quantity slowdown decided, 2026-11-10

- **Option 1 (owner, 2026-11-10)** (`5fec8fb57d`): the two canonical functions take the quantity's `value`, `system` and `code` as columns of their own, over the same per-thread cache and last-input check. `AnnotationKind` gains `compute(field, value, fieldOf)`, through which a caller that knows the schema passes the fields; the annotation step reads them from the structure it observes, and the engine through the tolerant traversal. No test expectation changed.
- **Re-measured:** on the cost probe both annotations are 9% to 11% above one grouped function, from 19% to 21%; on the engine probe annotating quantities is +6%, +13% and +26% (the last round carrying an order effect), a quantity comparison −9%, +3% and −12%, and the output-only date +6% to +9%. Date comparisons and searches stay faster.
- **The remaining cost is accepted (owner, 2026-11-10).** Box 6 is ticked.
- **For later, not built:** a cheaper last-input check and keying on the stored bytes, recorded as candidates for #2889 in the evidence file's open points; the ingest cost of flat annotations, recorded as a consideration in design §6, question 1. Ticket 46 lists no inputs to that question, so it is unchanged.

### Review fixes, 2026-11-10

- Since `5fec8fb57d` a quantity field that is not text is cast to text at ingest, where before it failed the canonicalisation with an error. Conformant data always has text there, so only the way wrong input fails changes. The kind's structure form, used where the caller does not know the schema, reads fields by name again, takes an absent one as null and still fails on one that is not text (`9ff7976fba`).
- A time comparison computes `LowBoundaryForTime` three times and `HighBoundaryForTime` twice per operand since the time comparator binds its operand with `SqlFunctions.let` (decision (c) above); no probe measures it. Both are recorded in the evidence.
- The flat-field ingest test gives each level a distinct quantity (`b1c6dfddd3`); one canonicalisation per quantity is counted for a repeating quantity at ingest and for the engine's conversion of a stored quantity; `RowFields`, now unused, is removed; and stale comments are corrected.

