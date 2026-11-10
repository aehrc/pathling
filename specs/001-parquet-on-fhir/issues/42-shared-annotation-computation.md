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
   - **System.Quantity** becomes `{value, unit, code, system}`, with `value` a `DECIMAL(38,6)` and no `id`, `comparator` or `_value` (decision 150). Its canonical fields sit flat beside them where they are available without computing: copied from a stored quantity, or constant for a literal. Otherwise they are computed where a comparison reads them, from the most exact source (decision 150). A System.Quantity returned to a caller as a column has the four content fields only (decision 150, amended 2026-11-10; for review in ticket 40). `contracts/engine-semantics.md` §Quantities describes the shape.
4. **Evidence.** `evidence/ticket-42-memoisation.md` records, on Spark 4.0.2:
   - the parse runs once per value, singular and repeating, inside a lambda, and when the fields are read in separate passes;
   - a traced input fires once;
   - throughput against today's grouping with `bindOnce`, on the existing benchmark. Any slowdown is brought to the owner before the ticket closes.

**Not in this ticket:**

- Grouped annotations (`__range{start, end}`, `__canonical{code, value}`) stay parked as design §6 question 1 (owner, 2026-11-10). The flat fields are built.
- The per-thread cache on Photon and Databricks is not checked here (owner, 2026-11-10). It goes to the profiling follow-up, #2889 (ticket 26). A miss costs a parse, never a wrong result.
- The lazy accessors, conversion, merges, comparison keys and emission shapes are ticket 43.

**Blocked by:** None. Ticket 14 is done.

**Status:** ready-for-agent

- [ ] The baseline plan tests are committed against today's code before any other change of this ticket or 43.
- [ ] `date_start` and `date_end` replace `date_range`. No struct-returning range computation and no `resolveFieldsOrElse` remain, and `bindOnce` remains only where it protects something other than a shared parse.
- [ ] Ingest writes `__canonical_code` and `__canonical_value_exact`, and nothing writes or reads `__canonical_exact`.
- [ ] Ingest tests show the flat fields written at every level: a quantity at the root (`Observation.valueQuantity`), under a repeating parent (`Observation.component.valueQuantity`), inside an extension (an extension's `valueQuantity`), and a repeating date under a repeating parent; each over a fresh schema and over data already carrying the fields.
- [ ] System.Quantity has the shape of decision 150, and a returned one has its four content fields only; `SystemQuantityShapeTest` asserts both.
- [ ] `evidence/ticket-42-memoisation.md` shows the three measurements above, and any slowdown has gone to the owner.
- [ ] Results are unchanged: the engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`).
