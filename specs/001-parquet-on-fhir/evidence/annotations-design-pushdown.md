# Which expression shapes let Parquet skip row groups for a date-bound filter

Measured for `design/annotations-in-the-engine.md` (sections 4, 5 and 7), ticket 43 finding 5, and
decision 138, whose rejection of `INT96` rests on "it never gets statistics or predicate pushdown".
The question is which shapes of `birthDate >= @2000`, stored as `_birthDate.__end >= 2000-01-01T00:00Z`,
make Parquet skip row groups. `PushedFilters` in the plan is not taken as evidence; each arm counts the
rows the scan emitted.

**Result.**

1. **Today's engine skips nothing.** `birthDate >= @2000` through the engine, as a FHIRPath
   filter, as `where(...).exists()`, and as the FHIR search `birthdate=ge2000` / `gt2000` through
   `DateMatcher`, scans all 1,200,000 rows in all 412 row groups. `PushedFilters` is `[]`. Nothing
   reaches Parquet.
2. **A plain field comparison skips.** `_birthDate.__end >= ts` scans 240,343 rows (84 of 412 row
   groups), which is exactly the rows of the row groups whose footer maximum reaches the bound. The
   same holds through the real `ColumnFunctions.resolveOrElse`, which is replaced by a plain
   `GetStructField` before pushdown, and through `struct(value, meta).meta.__end`, which the optimiser
   collapses.
3. **A `CASE WHEN` between the column and the comparison blocks pushdown.** So does the engine's
   own wrapper, which is a singleton `array`, `transform` lambda and `element_at` around the `CASE`
   (see the optimised plan below), and so do `coalesce(x, MAX) >= ts` and `coalesce(CASE ..., false)`.
4. **A comparison that is empty where it cannot compare is pushable only beside a plain superset
   conjunct.** `CASE WHEN canCompare THEN start >= ts END` alone scans everything; with `AND __end >= ts`
   (or `AND __start >= ts`), in either order, it scans 240,343 rows. Spark pushes the pushable
   conjuncts of an `AND` and keeps the `CASE` as a residual filter, so the answer is unchanged.
5. **`INT96` is skipped never.** `__end` as `INT96` has no footer statistics, so all 262 row groups are
   scanned, while `PushedFilters` lists the predicate exactly as for `TIMESTAMP_MICROS`. Decision 138's
   claim is confirmed by measurement, and ticket 44 may rest on it.
6. **Grouped against flat makes no difference.** `_birthDate.__bounds.upper` (two levels) scans the same
   244,770 rows of the same 40 of 189 row groups as `_birthDate.__end`. Parquet reads the leaf
   column's statistics wherever it sits.

## Conditions

- Commit `0c94065ec3` (the worktree was clean; the scripts are new files), Spark 4.0.2 (`local[2]`),
  Parquet from Spark's classpath, Java 21.
- 1,200,000 Patients (`gen_patients.py`: `id`, `birthDate` day precision rising from 1920-01-01 to
  2019-12-xx, one `name` with `family` and `period.start` equal to the birthDate). Encoded through the
  library API (`ptc.read().ndjson(..).write().parquet(..)`), which writes `_birthDate` as
  `{__start, __end}` of `TIMESTAMP(MICROS,true)`; the real layout holds no `id` or `extension` there for
  data without them. Then rewritten sorted by `birthDate` into one file with
  `parquet.block.size=262144`, so the row groups are narrow in `birthDate`. This is the best case for
  skipping, and the only one measured: unsorted data skips nothing under any shape.
- **412 row groups** in the full layout (84 reach the bound: 240,343 rows). Arms 6 and 7 use hand-built
  narrow tables of `id`, `birthDate`, `_birthDate` (189 row groups; `INT96`: 262), written by Spark from
  the same rows.
- `spark.sql.parquet.recordLevelFilter.enabled=false`, `spark.sql.adaptive.enabled=false`,
  `spark.sql.parquet.filterPushdown=true`. The scan's `numOutputRows` is the row count of the row groups
  that survived. It equals the footer-statistics expectation in every pushed arm, so no page-level
  skipping adds to it.
- Each arm collects the `id`s of the filter's matches, so the arm's answer is compared too. The bound is
  `2000-01-01T00:00:00Z`. The display of `PushedFilters` shows it in the JVM's zone.
- `bytesRead` in `output-1.2M.txt` is the Hadoop local-filesystem counter and is not usable: it is higher
  for the arms that skip (14.2 MB, against 10.2 MB for the arms that read everything), because the
  column and offset indexes and a footer read are counted. Rows scanned is the measure.

## Result by arm

"Engine" arms run through engine code; "hand-modelled" arms are Spark expressions written out to
isolate one factor, as the design's accessor would produce them.

| # | Arm | Ran through | `PushedFilters` shown | Rows scanned of 1,200,000 | Row groups |
| --- | --- | --- | --- | --- | --- |
| 1 | `birthDate >= @2000` (`fhirPathToColumn`) | engine | none | 1,200,000 | all 412 |
| 1 | `where(birthDate >= @2000).exists()` | engine | none | 1,200,000 | all |
| 1 | `birthDate.where($this >= @2000).exists()` (singular) | engine | none | 1,200,000 | all |
| 1 | `name.where(period.start >= @2000).exists()` (repeating) | engine | none | 1,200,000 | all |
| 1 | search `birthdate=ge2000`, `gt2000` | engine (`SearchColumnBuilder`, `DateMatcher`) | none | 1,200,000 | all |
| 2 | `_birthDate.__end >= ts` | hand-modelled | `IsNotNull`, `GreaterThanOrEqual` | **240,343** | 84 |
| 2 | `_birthDate.__start >= ts` | hand-modelled | yes | 240,343 | 84 |
| 2 | `ColumnFunctions.resolveOrElse(_birthDate, "__end", lit(EPOCH)) >= ts` | engine function from the jar | yes | 240,343 | 84 |
| 2 | the same, fallback `birthDate.cast(timestamp)` | engine function from the jar | yes | 240,343 | 84 |
| 3 | `struct(value, meta).meta.__end >= ts` | hand-modelled | yes | 240,343 | 84 |
| 4 | `when(birthDate.isNotNull, struct(..)).meta.__end >= ts` | hand-modelled | none | 1,200,000 | all |
| 4 | the same, with `resolveOrElse` inside the struct | hand-modelled, engine function | none | 1,200,000 | all |
| 5 | `CASE WHEN canCompare THEN start >= ts ELSE null` | hand-modelled (the `TemporalComparator` shape) | none | 1,200,000 | all |
| 5 | the same `AND __end >= ts` | hand-modelled | yes | 240,343 | 84 |
| 5 | the same `AND __start >= ts` | hand-modelled | yes | 240,343 | 84 |
| 5 | `__end >= ts AND CASE ...` (conjunct first) | hand-modelled | yes | 240,343 | 84 |
| 5 | `coalesce(CASE ..., false)` | hand-modelled | none | 1,200,000 | all |
| 5 | `CASE WHEN end IS NOT NULL THEN end >= ts` | hand-modelled | none | 1,200,000 | all |
| 5 | `isNotNull(CASE WHEN canCompare THEN ..)` (not in filter position) | hand-modelled | none | 1,200,000 | all |
| x | `coalesce(__end, MAX) >= ts` (`DateMatcher`'s Period shape) | hand-modelled | none | 1,200,000 | all |
| x | `__end >= ts OR __end IS NULL` | hand-modelled | `Or(.., IsNull)` | 240,343 | 84 |
| 6 | `__end`, `TIMESTAMP_MICROS`, narrow table | hand-modelled | yes | 244,770 | 40 of 189 |
| 6 | `__end`, `INT96`, narrow table | hand-modelled | `IsNotNull`, `GreaterThanOrEqual` shown | **1,200,000** | **all 262** (no statistics) |
| 7 | `_birthDate.__bounds.upper` / `.lower`, grouped | hand-modelled | yes | 244,770 | 40 of 189 |

Matches differ between arms by what they compute, not by pushdown: the FHIRPath `>=` and `gt` give
227,309 (a stored year of 2000 cannot be compared with `@2000`, so it is empty); `ge` search and a plain
`__end >= ts` give 239,342. A pushed arm returns the same matches as its unpushed twin in every row of
the table.

## Why the engine's filter is not pushed

The optimised filter of `birthDate >= @2000` is

```
element_at(transform(array(CASE WHEN isnotnull(birthDate) THEN named_struct(__start, _birthDate.__start,
  __end, _birthDate.__end) END), lambdafunction(element_at(transform([[946684800000000,978307199999999]],
  lambdafunction(CASE WHEN (x_1.__end < x_2.__start OR x_2.__end < x_1.__start OR (x_2.__start = x_1.__start
  AND x_1.__end = x_2.__end)) THEN x_1.__start >= x_2.__start END, ...)), 1, ...), ...)), 1, ...)
```

The stored bounds are read, but inside a `CASE`, inside a lambda over a one-element array
(`bindOnce` and `let`), inside `element_at`. Arm 4 shows the `CASE` alone is enough to block. This
measurement did not isolate the lambda and `element_at` on their own (no arm keeps them while removing the
`CASE`), so it does not say whether removing the `CASE` alone would suffice for the engine's form. It does
say that the three-part wrapper as built is opaque, and that the whole filter must reduce to a conjunct of
plain field comparisons for Parquet to see it.

## What this means for the design

- **The accessor.** `resolve_or_else(meta, "__end", ...)` is safe: it is a plain `GetStructField` by the
  time of pushdown, in the stored layout, and the fallback is not in the plan at all. Reading the bound
  directly, through the element struct (`struct(value, meta).meta.__end`), is also safe: the optimiser
  collapses a bare `CreateNamedStruct`. What must not sit between the column and the comparison is a
  `CASE WHEN` (`SystemDateTime.of`'s `when(text.isNotNull, struct(...))`), a `coalesce`, or a lambda.
  The null guard on the text is not needed to be correct: `__end` is null wherever the text is.
- **What a comparator emits.** A comparator that returns empty where it cannot compare (the three-valued
  result of `TemporalComparator`) cannot be pushed itself. Pushdown needs a separate, plain, positive
  **superset conjunct** beside it, on a stored bound, which Parquet can test and which a true result
  implies: for `ge`, `__end >= ts` (true results satisfy `start >= ts`, and `end >= start`). Spark pushes
  that conjunct and runs the `CASE` as a residual filter, in either order. Three conditions: the conjunct
  must be a conjunct of the top-level filter, not under `NOT`, `isNotNull`, `coalesce`, or a lambda (the
  last row of arm 5 does not push); it must be implied by a true result only, so it is sound only in
  positive polarity (under `NOT` or `OR` with other terms it would drop rows); and it must be over the
  stored column, not a computed range. Replacing the `CASE` by `canCompare AND p` would also push, but gives
  false rather than empty where it cannot compare, which changes `not()`, so it is not equivalent.
  Where a bound can be absent (`DateMatcher`'s Period), write `x >= ts OR x IS NULL`, which pushes, instead
  of `coalesce(x, MAX) >= ts`, which does not.
- **Ticket 44 (`INT96`).** Confirmed on measurement: `INT96` writes no min/max statistics, so no row
  group is skipped, while `PushedFilters` still lists the predicate. Decision 138's rejection of `INT96`
  stands, and the claim can cite this file. Anyone checking the plan for pushdown on an `INT96` store would be
  misled: only rows scanned shows it.
- **Grouped against flat.** No cost either way. A grouped `__bounds.{lower,upper}` placement prunes
  and skips as a flat `__start` and `__end` do. The choice between them need not weigh pushdown.
- **Cardinality.** A singular `birthDate.where(...)` and a repeating `name.where(period.start ...)` both
  scan everything today, the singular for the same reason as the plain comparison. A repeating date is
  inside an array and would not push even without the wrapper, as a lambda over an array is opaque to
  Spark; this was not measured separately.
- **What was not measured.** Unsorted data (skips nothing for any shape), the `id`-dependent
  `extension` fields of `_birthDate`, a table with several files and partitions, Delta (whose
  data skipping uses its own statistics, per file), and timing. The effect of the hint on a view's
  `where` at the top of a plan is not measured; only the filter column was.

## Reproduction

All under `scripts/annotations-design/pushdown/`; data under `.local/work/<branch>/spikes/pushdown/`.

```
python3 gen_patients.py <dir> 1200000                    # writes <dir>/Patient.ndjson
bash go.sh PushdownPrepare.java <ndjson dir> <work dir> 262144
bash go.sh PushdownProbe.java <work dir>                 # output-1.2M.txt is this run, minus stack frames
```

`go.sh` runs the probes with the jars of the worktree built at the commit above, and the Spark 4.0.2
classpath. `PushdownPrepare` writes the engine layout (`full`) and the narrow variants.
