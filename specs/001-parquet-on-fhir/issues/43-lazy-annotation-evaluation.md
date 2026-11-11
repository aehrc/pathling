# 43 — Evaluate annotations in the engine only where they are used

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-004, FR-021, FR-022, FR-023. Raised from the review of PR #2877, findings 2 and 5 (owner, 2026-10-29). Settled by the annotations design, `design/annotations-in-the-engine.md` §4.1 to §4.4 and §8, and decisions 140 to 144, 151 and 152. Examples: `design/annotations-by-example.md`. Split on 2026-11-11 (owner): decimals and System.Quantity are ticket 50, and the emission shapes, the level tests and the by-example tests are ticket 51.

**What to build:** The accessor model, for dates. Nothing is computed when a value is traversed. An operation that needs an annotation asks for it through an accessor, which reads the stored field or computes it where the schema lacks it; Spark makes that choice when it resolves the plan, so the engine stays schema-agnostic. The review's findings are what this fixes: a date's range is evaluated where only its text is output (finding 2), and a date comparison hides the stored bounds from pushdown (finding 5).

- **Deferred expressions.** `ResolveOrNone`, a new `RuntimeReplaceable` beside `ResolveOrElse` and `ResolveOrNull`, projects the named annotation fields a schema carries and leaves the others out: `struct(<content>, <present fields>)`, and for a repeating primitive `arrays_zip(<content>, <present fields>)` with `coalesce(x, array())` guards on the extracted arrays, so its fields match the singular form.
- **Element form (decisions 141 and 152), for dates.** A date, dateTime or instant is `struct(value, <stored __start and __end>)`, flat. The shape is fixed by the type, not the data, so a date over unannotated data is `struct(value)`. The raw `_x` is never carried. A literal carries its bounds as constants; a function result, such as `toDate()`, carries none (rule A). The FHIR to System conversion step (D8) drops `id`, `extension` and any other content the System type lacks; with the switch off a date carries none of them, so the step leaves a date's fields as they are, and building the step for the switch on is ticket 17.
- **Accessors (design §4.3).** Each bound is read from the element itself, `annotation_or_else(e, "__start", date_start(e.value))`, over ticket 42's memoised UDFs. `SystemDateTime`'s eager range goes. The accessor reads the stored field past the traversal's null guard, which relies on the trust contract: an annotation is null exactly where its value is null (ticket 31). It is what lets date filters push down (owner, 2026-11-11).
- **Merges (decision 142, D9, and decision 152).** `MergeCast` and `UnresolvedMergeCombination` keep only the annotation fields every operand has, at every depth, and leave out a structure the intersection empties, which stores nothing and which Parquet cannot write (owner, 2026-11-11). Dates merge through the reconciler. Dropping `id` and `extension` for a System result is ticket 17.
- **Comparison keys (decision 151).** Deduplication compares a key computed once per element, keeping the first element: a date's text where it has no time part, and otherwise its range; another complex type's element struct, with another producer's `__` fields stripped at every depth by a projection planned from the schema; nested `id` and `extension` stay. Quantity, Coding and times keep their pairwise comparison. A primitive's annotation fields do not affect `=`, `distinct()` or `|`. For a HumanName, `|` ignores another producer's `__` fields, and `=` and `distinct()` stay rejected as on main; equality on complex types stays with the equality review in ticket 26 (owner, 2026-11-11).
- **Output (decision 142, D10, and design §4.5), for dates.** A date leaves the engine as its value only, its text. Its annotation fields are never output. Ticket 50 keeps this for decimals and quantities.
- **Pushdown (decision 144).** A date comparison that is empty where it cannot compare is emitted beside a plain superset conjunct on the stored bound, such as `CASE WHEN canCompare THEN start >= ts END AND __end >= ts`, in `TemporalComparator` and in `DateMatcher`'s scalar path, whose `ge` keeps main's matching. A scalar search match is made false where it is null by `isnotnull(m) AND m`, so that its conjuncts reach the scan (owner, 2026-11-11). `bindOnce` leaves both.

**Not in this ticket:**

- Decimals, System.Quantity, and box 3's test over each annotated type: ticket 50.
- The emission shapes (L2), the level tests and the by-example tests: ticket 51.
- The D14 switch for a primitive's `id` and `extension`, and dropping them for a System merge result: ticket 17.
- Whether `DateMatcher`'s `ge` matches the FHIR search specification: ticket 31.
- Emitting the conjunct only where it can be pushed down, and binding the bounds once with `let`: #2889.
- Shaping elements by use (L1, decision 140) and Pathling's own optimiser rule (D3b, decision 143; #2890).
- Presence-driven algorithm choice (decision 148), and which form is the truth for a computed System value (design §6 question 6; #2892).
- Grouped annotations, parked as design §6 question 1 (owner, 2026-11-10).

**Blocked by:** 42, for the baseline plan tests and `date_start` and `date_end`.

**Status:** done

- [x] Ticket 42's baseline plan tests for dates reach their target, and each change from the baseline is explained: an output-only date under a repeating parent, and after `where()`, has no date computation in the optimised plan; a stored date merged with a literal gives the results of `design/annotations-by-example.md` §5.1; `birthdate=ge2000` and `where(birthDate >= @2000)` over annotated Parquet push a filter on `__end` to the scan. The quantity merge moves to ticket 50.
- [x] `ResolveOrNone` leaves out the fields the schema lacks, in both its forms, and its repeating form carries the singular names (`ResolveOrNoneTest`).
- [x] The accessor reads a stored bound past the traversal's null guard, and not past any other guard, and a test names its reliance on the trust contract (`ResolveOrNoneTest`).
- [x] A merge keeps only the annotation fields every operand has, at every depth, and a merged value whose structure the intersection empties can be written to Parquet and read back with the same results (`MergeCastTest`).
- [x] For a HumanName, `|` ignores another producer's `__` fields, and `=` and `distinct()` stay rejected as on main, with their error unchanged (`ComparisonKeyTest`). A primitive's annotation fields do not affect `=`, `distinct()` or `|`: dates stored with their bounds, without them, and with another producer's annotation beside them give the same results (`DateDeduplicationTest`). Quantity and Coding behave as on main: their comparison is unchanged, and `CombiningOperatorsDslTest.testQuantityUnion` and the Quantity and Coding groups of `ExistenceFunctionsDslTest` pass as they are.
- [x] A date leaves the engine as its text only, singular and repeating, over annotated data (`DateDeduplicationTest.aDateLeavesTheEngineAsItsTextOnly`).
- [x] A date under a singular parent and a choice date push a filter on the stored bound; a date in an extension is compared inside a lambda, which Parquet cannot test, so nothing is pushed there (`DatePushdownTest`).
- [x] Dates are deduplicated on a key computed once per date with the results of the pairwise comparison: equal dates, different precisions kept, one instant in different time zones, seconds against milliseconds, empty and singleton collections, under a repeating parent, over annotated and unannotated data (`DateDeduplicationTest`).
- [x] A date comparison is empty where it was, under `not()` as elsewhere, with the plain conjunct beside it (`DateRangeComparisonTest`).
- [x] `SystemDateTime` builds no eager range, and `TemporalComparator` and `DateMatcher` contain no `bindOnce`.
- [x] The engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`), and `evidence/ticket-43-lazy-annotations.md` records the before and after figures.

## Comments

### Partial implementation, 2026-11-10 (branch issue/2832_43_lazy-annotations)

Three of the design's slices are built: the deferred expressions, dates end to end, and the
comparison key for complex values. Decimals, System.Quantity, the L2 emission shapes and the
level tests are not started. Box 8 is ticked; the others are not met.

**Built.**

- **Deferred expressions** (`b1e14cb405`, `encoders`). `ResolveOrNone` projects the named fields a
  source's schema carries and leaves the others out, in a singular form, `struct(<content>, <present
  fields>)`, and a repeating one, `arrays_zip(<content>, <present fields>)` with each extracted field
  guarded by `coalesce(field, array())`, so both forms carry the singular names. `AnnotationOrElse`
  is the accessor: the field where the element carries it, read through the traversal's
  `CASE WHEN isnotnull(value)` guards so that Parquet can push a filter on it, and the computation
  otherwise. `MergeCast` keeps a `__` field only where every operand has it, at every depth.
- **Dates** (`f88d388b73`, fixed in `a73b40542e`). A date is `struct(value, <stored __start and
  __end>)`, nothing computed at traversal; a literal carries its constants; `toDate()`,
  `toDateTime()` and terminology results carry none (rule A). `SystemDateTime.start` and `end`
  are the accessors. `TemporalComparator` reads them and emits beside its `CASE` a plain conjunct
  that every true result implies and that holds wherever the comparison is empty, so the result is
  unchanged in every row, under `not()` as elsewhere (decision 144). `DateMatcher`'s `ge` adds the
  conjunct on the upper bound. `SearchFilter` makes a scalar match false where it is null with
  `isnotnull(m) AND m` rather than `coalesce(m, false)`, which hid every conjunct from pushdown. Dates
  merged in `|` and `combine()` go through the reconciler under a flat canonical structure, so the
  intersection applies.
- **Comparison keys** (`d22d060ac7`, narrowed in `a73b40542e`). Deduplication of values compared
  with default SQL equality (`|`, `distinct()`, `repeat()`) is `DistinctOnKey`: Spark's
  `array_distinct` where the element type has no unrecognised `__` field, and otherwise a key with
  those fields stripped at every depth, computed once per element, keeping the first element. A key
  for temporal values was built and measured slower than the pairwise comparison, so it was
  removed (see the evidence).
- **Tests.** `ResolveOrNoneTest` (7), `DistinctOnKeyTest` (3), two `MergeCastTest` cases,
  `ComparisonKeyTest` (5), and six new `DateRangeComparisonTest` cases (negation over empty
  comparisons, a merge, and a `where()` on another element).
- **Evidence.** `evidence/ticket-43-lazy-annotations.md`, `evidence/scripts/ticket-43/`.

**Awaiting the owner.**

1. **Changed expectations** (each explained in the test).
   - `AnnotationPlanBaselineTest`, in `a1fe64b397`: the two output-only tests over unannotated data
     now compute no range (renamed `...ComputesNoRange...`); the date merge keeps the bounds over
     annotated data and is `struct<value>` on both sides over unannotated data, as
     `annotations-by-example.md` §5.1 has it (renamed `...KeepsTheBoundsBothSidesHave`); the two
     pushdown tests assert the positive form (renamed `...PushesAFilter...`).
   - `AnnotationMemoisationTest.aRepeatingDateConvertedInTwoPassesParsesEachDateOnce`, in
     `a1fe64b397`: renamed `aRepeatingDateReadInTwoPassesParsesEachDateOnce`, it reads the bounds
     through the accessors, because traversal computes none; the count (1500) is unchanged.
   - `DateRangeComparisonTest.theComputedRangeTakesOneParse` (2 cases): five bound computations in
     the plan became six, the extra one in the plain conjunct; at run time no more parses than rows.
     This change is in the feature commit `f88d388b73`, not in `a1fe64b397` as its message says.
2. **Slowdowns, over unannotated data** (three rounds at `a73b40542e`, evidence file). A union
   of dates is 15% to 19% slower, a date's `distinct()` 5% to 8%, a comparison inside a lambda 5%
   to 12% (+1% and +0% at the date slice alone), and a date search 3% to 13%. The first two come
   from the merge and traversal now leaving dates without bounds, so the pairwise deduplication
   computes them for each pair; a key computed once per element was slower still (+22% to +68%)
   and was removed. Over annotated data nothing is slower beyond the spread, and an output-only
   date under a repeating parent is 78% faster over unannotated data. Not fixed.
3. **Design points decided here, for review.**
   - The merge intersects `__` fields at every depth; an operand that lacks the structure an
     annotation is in lacks the annotation. This can leave an empty structure, such as
     `_value: struct<>`.
   - The accessor is `AnnotationOrElse`, not plain `resolve_or_else`: it sees through the
     traversal's guards on the element's value (and on a present constant), which is valid because
     an annotation is null wherever its value is under `trust`. Without it no filter is pushed.
   - `DateMatcher`'s `ge` reads the lower bound, while the design says `ge` is decided by the upper
     one; the conjunct on `__end` was added to meet box 1, beside the `__start` filter that is
     pushed anyway.
   - `SearchFilter`'s scalar `coalesce(m, false)` is replaced, outside `DateMatcher`, because it
     blocked pushdown of every search.
   - "Drop `id` and `extension` for a System result" is not built: every operand is converted
     before a System merge, and with ticket 17 not built no primitive carries them.
   - Box 6's `=` and `distinct()` on a HumanName are rejected on main ("Unsupported equality for
     complex types"), so only `|` is tested.

**Not built.** Decimals (the text element, `numeric(e)`, detection, `toString()`), System.Quantity's
decision-150 shape and the four-field return, the L2 emission shapes and existence rewrites, the
level tests of box 7, and the tests of `annotations-by-example.md` §3 to §5 beyond the dates.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at `a73b40542e`, all green:
utilities 40, fhir-schema 62, encoders 617, io 797 (1 skipped), legacy-export 38, terminology 926,
fhirpath 7957 (888 skipped), library-api 379. `AnnotationFreeSuiteTest` (15) passes within it.
Python and R were not run. **Status** stays ready-for-agent.

### Owner's decisions and the split, 2026-11-11

- **Approved:** every changed expectation in `a1fe64b397`, and `DateRangeComparisonTest.theComputedRangeTakesOneParse` going from five computations to six, which is in `f88d388b73`. The conjunct stays as built; emitting it only where it can be pushed down, and binding the bounds once with `let`, are #2889.
- **The date deduplication slowdown is fixed** (`bbd458ce50`). Each date's key is computed once: its text where it has no time part, and otherwise its range through the accessors; an element is kept where `array_position` of its key is its own position. Over unannotated data a union of dates and a date's `distinct()` are now 32% to 37% faster than before the ticket, where pairwise deduplication had made them 15% to 19% and 5% to 8% slower. Over annotated data a deduplication is 4% to 11% slower, a comparison inside a lambda over unannotated data stays 5% to 10% slower, and a union of complex values is 5% to 21% slower in the two rounds. Figures are in the evidence file. `DateDeduplicationTest` gives the same results with the key and with the pairwise comparison.
- **Merge depth:** the intersection stays at every depth, and a structure it empties is left out (`7803d99bd6`).
- **Accessor guards:** accepted, and recorded in design §4.3; `ResolveOrNoneTest.accessorReliesOnAnAnnotationBeingNullWhereItsValueIs` names the reliance on the trust contract (`0a83dc0a19`).
- **`ge`:** main's matching and the `__end` conjunct stay; ticket 31 has a box to check `ge` against the FHIR search specification; the design's sentence says what is built.
- **`SearchFilter`'s `isnotnull(m) AND m`:** accepted.
- **Dropping `id` and `extension` for a System merge result:** moved to ticket 17, with a box.
- **Box 6:** amended; `ComparisonKeyTest` pins that `=` and `distinct()` on a HumanName are still rejected with the same error (`d68a6f2187`).
- **Split.** This ticket closes at the dates slice, and its scope and boxes above are what was built. Decimals, System.Quantity and the test over each annotated type (former boxes 3, 4 and 5) are ticket 50, with the handovers from tickets 41 and 42. The emission shapes, the level tests and the by-example tests (former boxes 2, 7 and 9) are ticket 51. References to this ticket for the moved work, in `contracts/engine-semantics.md`, the design and tickets 15, 16, 41 and 46, point at them.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at `d68a6f2187`, all green: utilities 40, fhir-schema 97, encoders 619, io 797 (1 skipped), legacy-export 38, terminology 926, fhirpath 7965 (888 skipped), library-api 379. `AnnotationFreeSuiteTest` (15) passes within it. Python and R were not run.

### Review fixes, 2026-11-11

- A keyed deduplication keeps a single null element, as `array_distinct` does (`8f1ad027b9`; `DistinctOnKeyTest` covers empty and null arrays and null elements on both branches).
- The date key's reliance on FHIR R4's rule that a date or dateTime without a time has no offset is recorded in design §4.3 and in `TemporalComparator`; `2020-01-01Z` against `2020-01-01` keeps its own key, as the owner decided.
- Restored from before the split: a primitive's annotation fields do not affect `=`, `distinct()` or `|`, and Quantity and Coding behave as on main (box 5); nested `id` and `extension` stay in a complex value's key; a date leaves the engine as its text (a new box). Ticket 17 now covers the D8 conversion step's drop of `id` and `extension` as well as merges.
- Pushdown at more levels (`DatePushdownTest`): a date under a singular parent and a choice date push a filter on the stored bound; a date in an extension is compared inside a lambda, so nothing is pushed there.
- The evidence file's text is corrected: the complex-union figure is spread, the searches show no large regression and the method's limits are recorded, and the jars are named by commit.
- Comments corrected in `DateMatcher` (the `ge` conjunct relies on invariant per-1, start ≤ end), `ColumnEquality`, `TemporalComparator` and the design; long lines wrapped in `AnnotationOrElse`.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` after these fixes, all green: utilities 40, fhir-schema 97, encoders 620, io 797 (1 skipped), legacy-export 38, terminology 926, fhirpath 7970 (888 skipped), library-api 379. `AnnotationFreeSuiteTest` (15) passes within it.

