# Quantity at the resource root, absent root columns, and the existence wrapper

Measured for `design/annotations-in-the-engine.md` (sections 4.3, 4.7 and 6) on local Spark 4.0.2. Three
questions: what a singular complex top-level column (`Observation.valueQuantity`) reads and whether Parquet
can skip row groups on a quantity comparison (A); whether the tolerant reference copes with absent root
columns, including a prototype of `columnOrElse` (B); and whether the existence wrapper blocks pushdown (C).
Read schemas are the executed plan's `FileSourceScanExec.requiredSchema`; pushdown is judged by the rows the
scan emitted, on data sorted into narrow row groups, as in `annotations-design-pushdown.md`.

**Summary of findings.**

**A. A whole-quantity engine output reads 9 leaves and never `extension` or a foreign field (`__foreign`); a
quantity comparison reads 10 leaves, 6 more than its canonical form needs; the engine's filter scans
200,000 of 200,000 rows, and a sound plain predicate on FlexiDecimal's leaves also scans all 200,000, because
unscaled values are not ordered across scales. Only a fixed-scale field skips (5,039 rows for 4,999 matches).**
Hand-built `named_struct` of the non-`__` fields prunes `__canonical_exact` and `__foreign`, plain and under
`inline`; the same projection over the engine's output does not (the engine's `cast(struct)` blocks it). The
pruning rule changes no read schema and no result in any of the 17 cases.

**B. The engine already tolerates an absent `_birthDate` and an absent `valueQuantity` (same results, reads
only `birthDate` or `id`). `columnOrElse` takes 33 lines modelled on `UnresolvedColumnOrNull`, or 11 lines
composed over the existing `columnOrNull(name, NullType)`; both give the `_x`-layout results (18,817 rows),
read only the columns present, and a null fallback is wrong (0 matches against 19,840 for an end-bound
comparison).** The pruning rule is transparent in all 15 B cases.

**C. The existence wrapper `CASE WHEN id IS NOT NULL THEN _birthDate.__end END >= ts` blocks pushdown (1,200,000
of 1,200,000 rows scanned against 240,343 for the plain comparison), and the D11 conjunct restores it
(240,343). The engine's own `birthDate >= @2000` has no `isnotnull(id)` at all: its existence column is
`lit(true)`, so the wrapper is folded away at analysis; its filter is still opaque (1,200,000 rows).**

## Conditions

- Commit `0c94065ec3` plus uncommitted design files; Spark 4.0.2 `local[2]`, Java 21, session time zone UTC
  for part B. `recordLevelFilter` off, `filterPushdown` on, AQE off. Nothing here is timed, so the bench lock
  was not used.
- **Observations (A).** 200,000, `valueQuantity` rising by 0.001 mg per row (`gen_observations.py`), each with a
  quantity `id` and an `extension`. Encoded through the library API, so `valueQuantity` carries
  `id, extension, value, _value.__numeric, unit, system, code, __canonical_exact{value{value decimal(38,0),
  scale int}, code}`. The canonical form is in grams; its scale is 6 for 180,000 rows, 5 for 18,000, 4 for
  1,800, 3 for 180, and 2, 1, 0 for the rest, because trailing zeros are stripped. Two variants, sorted by
  `_value.__numeric`, `parquet.block.size=262144`:
  - `obs_engine`: today's layout plus `__foreign`, a 200-character string different in each row. 284 row groups.
  - `obs_flat`: the proposed layout. `__canonical_exact` replaced by `__canonical_code` and
    `__canonical_value_exact` (the same FlexiDecimal struct), plus `__foreign`, plus an optional fixed-scale
    `__canonical_value decimal(38,12)` of my choosing (section 6, question 4). 296 row groups.
- **Patients (B, C).** C uses the pushdown evidence's 1,200,000 Patients in 412 row groups. B uses a sample of
  99,819 of them (`hash(id) % 12 = 0`, order kept) in four variants: with `_birthDate`; with neither
  column (`pat_none`); with top-level `__birthDate_start` and `__birthDate_end` (`pat_flat_both`); with only
  `__birthDate_start` (`pat_flat_start`).
- The engine does not know the flat layouts. Engine arms on `obs_flat` therefore exercise today's fallback
  (computing the canonical form from `value`, `system`, `code`); proposed-layout arms are hand-modelled and
  marked so.
- "Rule" is `LambdaPruning.install(spark)` from `annotations-design-lambda-pruning-rule.md`, compiled
  unmodified. Each case is run without and with it; results are compared by row count and a hash of all rows.

## Part A: Quantity at the root

### A1 and A4: output, with and without the rule

The read schema is the same with and without the rule in every row below, and every result is identical.
The 8-leaf `valueQuantity` of `obs_engine` has `extension` (2 leaves), `__foreign`, and `__canonical_exact` (3 leaves).

| Case (`obs_engine`) | Leaves read of `valueQuantity` | `extension` | `__foreign` |
| --- | --- | --- | --- |
| Engine, whole: `fhirPathToColumn("value.ofType(Quantity)")` | `id, value, _value.__numeric, unit, system, code, __canonical_exact.{value.value, value.scale, code}` | no | no |
| Engine, view column `value.ofType(Quantity).code` (inline wrapper) | `code` | no | no |
| Engine, view columns `.value` and `.code` | `_value.__numeric, code` | no | no |
| Hand: `named_struct` of non-`__` fields | `id, extension[].url, extension[].valueString, value, _value.__numeric, unit, system, code` | yes (output) | no |
| Hand: `inline(array(named_struct))` | the same | yes (output) | no |
| Hand, over the engine's output: `named_struct` of its non-`__` fields | `id, value, _value.__numeric, unit, system, code, __canonical_exact.*` | no | no |

Measured:

- The engine's quantity output is the System quantity shape (`id, value, _value, comparator, unit, system, code,
  __canonical_exact`), so `extension` and any foreign field are not in it and are never read.
- A view cannot output a whole quantity: `value.ofType(Quantity)` as a column fails with "cannot obtain value
  for non-primitive collection". The whole-quantity row is therefore the select column of `fhirPathToColumn`.
- The hand-built strip of `__` fields is a singular `named_struct` with no lambda and prunes, as expected, in a
  plain projection and under `inline`. The strip over the engine's own output does not prune
  `__canonical_exact`: the engine builds the quantity as `cast(struct(...) as struct<...>)`, and Spark does not
  collapse a field extraction through the `cast`. The rule does not either (its demand analysis has no `Cast`
  case, so it treats the cast's input as read whole).
- On `obs_flat` the engine reads the same columns minus the canonical ones, and ignores the flat fields.

### A2: `value.ofType(Quantity) > 5 'mg'` in filter position

194,999 matches, identical through `fhirPathToColumn` and a view `where`, with and without the rule.

| Case | Leaves read of `valueQuantity` (plus top-level `id`) |
| --- | --- |
| Engine, `obs_engine` | `id, value, _value.__numeric, unit, system, code, __canonical_exact.{value.value, value.scale, code}`: 10 leaves |
| Engine, `obs_flat` (stored canonical form ignored) | `id, value, _value.__numeric, unit, system, code`: 6 leaves |
| Hand-modelled proposed accessor over `obs_flat`: `compare_canonical(struct(__canonical_value_exact, __canonical_code), 5 mg) > 0` | `__canonical_code, __canonical_value_exact.value, __canonical_value_exact.scale`: 3 leaves |

The engine's optimised filter builds the whole System quantity (`cast(struct(id, vq.id, value, vq.value, _value,
..., __canonical_exact, vq.__canonical_exact) as struct<...>).__canonical_exact`) inside two `CASE WHEN
isnotnull(valueQuantity)` guards, then takes the one field. Because of the `cast`, all of its fields are read
for the canonical form that only needs three leaves. `extension` and `__foreign` are still not read. The
hand-modelled accessor omits the engine's `coalesce` fallback to the original value, so its 3 leaves are a
lower bound and not what the engine would read.

### A3: pushdown of a quantity comparison

Bound 195 mg (about 4,999 matches of 200,000), so skipping has room to show. `u` is
`__canonical_exact.value.value` (`__canonical_value_exact.value` in the proposed layout), `scale` its sibling.
195 mg is 0.195 g, FlexiDecimal (195, 3). Both leaves have footer statistics, and the predicates are pushed
(`PushedFilters` lists them), so being nested in a struct and being `decimal(38,0)` do not prevent pushdown.

| Layout | Arm | Rows scanned of 200,000 | Matches (engine: 4,999) |
| --- | --- | --- | --- |
| today, 284 groups | (i) engine filter | **200,000** (`PushedFilters: []`) | 4,999 |
| today | (ii) plain `u > 195000` (the bound at scale 6) | 5,142 | **4,500, unsound** |
| today | plain `u > 195` (the bound's own unscaled value) | 200,000 | 199,276 (superset) |
| today | superset `scale < 6 OR u > 195000` | 200,000 | 24,500 (superset) |
| today | superset, conjunction over k = 3..6 of `scale < k OR u > 195 * 10^(k-3)` | 200,000 | 5,019 (superset) |
| today | engine AND that conjunction (D11) | 200,000 | 4,999 |
| proposed, 296 groups, hand-modelled | accessor over flat fields | 200,000 | 4,999 |
| proposed | plain `__canonical_value_exact.value > 195000` | 5,039 | **4,500, unsound** |
| proposed | superset conjunction k = 3..6 on the flat leaves | 200,000 | 5,019 (superset) |
| proposed | accessor AND the conjunction | 200,000 | 4,999 |
| proposed | optional fixed scale: `__canonical_value > 0.195` | **5,039** | 4,999, exact |
| proposed | accessor AND `__canonical_value >= 0.195` (D11) | **5,039** | 4,999 |
| proposed | accessor AND `__canonical_code = 'g'` AND `__canonical_value >= 0.195` | 5,039 | 4,999 |

What in FlexiDecimal allows or prevents a plain pushed predicate (measured unless marked):

- **Allows:** the leaves are plain scalar columns with statistics, so a predicate on them is pushed and skips.
  `u > 195000` scans 5,142 rows.
- **Prevents correctness:** `u` is an unscaled integer, ordered like the number only among rows of one scale.
  Trailing zeros are stripped, so 10% of these rows are at scale 5 or below, and `u > 195000` loses 499 of the
  4,999 matches. A crafted table (`(25,2) (5,1) (3,0) (3000,3) (7,3) (4,3)`) shows the same: `value > 0.195` gives 4
  rows, `u > 195` gives 1.
- **Prevents skipping:** a sound superset can be written. For a positive bound `ub / 10^sb` and a row of scale
  `s >= k >= sb`, `x > b` implies `u > ub * 10^(k - sb)`, so `scale < k OR u > ub * 10^(k - sb)` never loses a
  match (5,019 matches against 4,999). It is pushed, but it skips nothing here: the row groups hold mixed scales,
  so the footer minimum of `scale` is below `k` in every group and the first disjunct is never refuted (200,000
  rows scanned). I did not read the per-group `scale` statistics directly; that follows from the scan counts and
  the scale distribution.
- **The D11 pattern works for correctness, not for skipping, on FlexiDecimal:** engine AND the conjunction returns
  the engine's 4,999 rows and scans everything.
- **A fixed-scale field is exact and skips.** `__canonical_value > 0.195` scans 5,039 rows and returns exactly the
  4,999. The cost, not measured, is the one FlexiDecimal avoids: a fixed scale cannot hold both very large and
  very small canonical values, and a comparison at the rounded value is a superset only if the bound is rounded
  the safe way. The unit guard `__canonical_code = 'g'` is a plain pushable conjunct and costs no skipping.
- The superset must also hold for a negative bound (the argument above assumes a positive one); not tested.

## Part B: absent root columns and the tolerant reference

### B1: the engine over absent columns, and `resolve_or_none`

All rows are with and without the rule, and identical.

| Case | Read | Result |
| --- | --- | --- |
| No `_birthDate`: `Patient.birthDate` | `birthDate` | 99,819 rows, same hash as the dataset with `_birthDate` |
| No `_birthDate`: `Patient.birthDate > @2000` | `id, birthDate` | 18,817 matches, same as with the column |
| No `valueQuantity`: `value.ofType(Quantity)` | `id` | 200,000 rows, the column null |
| No `valueQuantity`: `value.ofType(Quantity) > 5 'mg'` | `id` | 0 rows, no error |

The engine computes the date bounds from `birthDate` when `_birthDate` is absent and returns the stored-layout
answer, and reads nothing of the absent quantity.

A hand model of `resolve_or_none` (`ResolveOrNone` in `Prototypes.scala`, a `RuntimeReplaceable` that keeps the listed
fields the child's resolved struct type has) over `columnOrNull("_birthDate", NullType)`:

| `_birthDate` | `resolve_or_none` type | `resolve_or_else(ann, "__start", computed)` | `resolve_or_null(..., "__start")` |
| --- | --- | --- | --- |
| present | `struct<__start, __end>` | stored; 0 of 99,819 rows differ from the computation | stored; 0 differ |
| absent | `struct<>` (row value `{}`) | computed; 0 of 99,819 differ | **null in all 99,819 rows** |

An absent container resolves to the null type, `resolve_or_none` gives it no annotation fields, and the accessor
computes. `resolve_or_null` gives a null `__start`, which the accessor would read as a stored absent value.

### B2: the flat parent-level layout and `columnOrElse`

`birthDate >= @2000` is modelled as `TemporalComparator` shapes it (`CASE WHEN canCompare THEN start >= ts END`),
with `start` and `end` from `columnOrElse(name, computed)` where the computation is a Spark SQL expression of
`birthDate` (day precision, so a stand-in for the engine's `date_range`). The reference is the same comparison
over `_birthDate.__start` and `__end`: 18,817 rows. Each form below returns exactly the reference (same hash)
with and without the rule.

| Columns present | Direct (modelled on `UnresolvedColumnOrNull`) | Composed over `columnOrNull(name, NullType)` | Set form + accessors | Leaves read (all forms) |
| --- | --- | --- | --- | --- |
| both | = reference | = reference | = reference, struct `<__start, __end>` | `id, __birthDate_start, __birthDate_end` |
| start only | = reference | = reference | = reference, struct `<__start>` | `id, birthDate, __birthDate_start` |
| neither | = reference | = reference | = reference, struct `<>` | `id, birthDate` |

When a column is present the fallback is not in the plan, so `birthDate` is not read; when it is absent the
column is not read. `id` is only the output.

**A null fallback is wrong.** With `__birthDate_end` absent, as a filter:

| Form | `end >= @2000` matches (reference 19,840) | `birthDate >= @2000` matches (reference 18,817) |
| --- | --- | --- |
| `columnOrNull` (null fallback) | **0** | 18,817 |
| `columnOrElse` | 19,840 | 18,817 |

Row `p844562`, `birthDate` 1990-05-01, stored start 1990-05-01: with the null fallback `end` is `NULL`, and both
comparisons are `NULL` (empty); with `columnOrElse` `end` is `1990-05-01T23:59:59.999999` and both are `false`.
The `CASE` comparison's filter count agrees only because the post-2000 rows are decided by `start`; the empty
against `false` difference remains for every earlier row, so it shows in `not()`, `exists()` and output.

**The prototype.** `Prototypes.scala` is 126 lines in all, with its comments.

| Piece | Lines | Notes |
| --- | --- | --- |
| `UnresolvedColumnOrElse` (direct) | 33 | Copies `UnresolvedColumnOrNull.mapChildren`: resolve the name as a plain attribute; else resolve the view column and catch absence; the absent branch returns `f(orElse)` instead of a null literal |
| `ColumnOrElse` (composed) | 11 | A `RuntimeReplaceable` over `columnOrNull(name, NullType)` and the fallback: replacement is the fallback where the child's type is `NullType`, as `ResolveOrElse` chooses by type |
| `ColumnsStruct` (set form) | 11 | `RuntimeReplaceable` building a `named_struct` of the children that did not resolve to `NullType` |
| `ResolveOrNone` | 14 | as above |
| Bridge, entry points | 27 | |

Spark internals the direct form depends on, beyond what `UnresolvedColumnOrNull` already depends on (it reuses its
`SOURCE_NAME`, `NO_MATCH` and `isSingleChildAssertion`): the `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` error and its
`actualCols` message parameter as the absence signal, the single-child `AssertionError` recognised by a stack
frame in `ColumnResolutionHelper`, `GetViewColumnByNameAndOrdinal`, and the analyser applying a rule through
`TreeNode.mapChildren`. It also needs the fallback to be a **child**: the first version kept it as a field, and
the absent branch then failed at execution with "Cannot generate code for expression: columnnodeexpression",
because a Column converted by `ExpressionUtils.expression` is a `ColumnNodeExpression` that the analyser only
converts inside the tree. The ambiguity branch (`resolveMatches`, private) is not copied; Spark's own error
stands. The composed form depends on none of these directly, only on the unchanged `columnOrNull`, on
`RuntimeReplaceable`, and on `NullType` meaning absent. It treats a stored column of `NullType` as absent;
Parquet cannot hold one, but an in-memory dataset can. The composed form is the smaller change and I would use it.
`ColumnFunctions` uses `ExpressionUtils`, which is `private[sql]`, so a Scala prototype outside the engine needs a
bridge in `org.apache.spark.sql.*` (the `Bridge` object); production code in Java does not.

### B3: the rule

All 15 cases of B1 and B2 (each without and with the rule) give the same results and the same read schemas; it
reads no wider and no narrower anywhere. B1's absent-column cases already read only `id` or `birthDate`, so
there was nothing for it to prune.

## Part C: does the existence wrapper block pushdown?

1,200,000 Patients, 412 row groups, 84 reach the bound. Matches are 239,342 for the plain comparison and 227,309
for the engine, which cannot compare a stored year-2000 date with `@2000` (as in the pushdown evidence).

| Arm | Optimised filter | Rows scanned |
| --- | --- | --- |
| C1 `_birthDate.__end >= ts` (control) | `isnotnull(__end) AND __end >= ts` | **240,343** |
| C2 `CASE WHEN id IS NOT NULL THEN __end END >= ts` | `CASE WHEN isnotnull(id) THEN (__end >= ts) END` | 1,200,000 |
| C2' `CASE WHEN id IS NOT NULL THEN __end >= ts END` | the same | 1,200,000 |
| C3 C2 AND C1 (D11) | `isnotnull(__end) AND CASE ... AND __end >= ts` | **240,343** |
| C3' C1 AND C2 | the conjuncts in the other order | 240,343 |
| C4 engine `birthDate >= @2000` | `element_at(transform(array(CASE WHEN isnotnull(birthDate) THEN named_struct(__start, __end) END), ...` | 1,200,000 |

- Spark rewrites the wrapper's `CASE` to the same filter whether the comparison is inside or outside it, and does
  not turn it into `id IS NOT NULL AND ...`. Only a plain conjunct is pushed, and C3 pushes it.
- **`isnotnull(id)` does not survive in C4 because it is never there.** The analysed plan has `CASE WHEN
  isnotnull(true) THEN ...`. `fhirPathToColumn` and a view's `where` resolve the resource with
  `ResourceRepresentation.alwaysPresent()`, whose existence column is `lit(true)`, and `existing()` is then
  `when(true IS NOT NULL, column)`, which the optimiser folds. The same holds in a view over `pat_x`.
  `withIdColumn()` has no caller in `fhirpath/src/main` (grep). It is not that `id` is non-nullable; Parquet
  reads it as nullable, as C2 shows.
- C4 is not pushed because of the date comparison's `array`, lambda and `CASE` (the pushdown evidence), not the
  wrapper. If `withIdColumn()` were used, the wrapper would add one more blocker, which C3's conjunct handles.

## Risks and gaps

- All quantities are in one unit (mg to g), values rise monotonically, and the data are sorted by value into
  narrow row groups: the best case. Unsorted data skips nothing under any shape. No page-level skipping, no
  Delta, no Databricks.
- Mixed scales come from stripped trailing zeros of one decimal series. Real data with other units would mix
  canonical codes too; the unit guard is a pushable conjunct but I did not test several codes in one group.
- `__canonical_value decimal(38,12)` is my choice, not the spec's scale; its pushdown is measured, its
  precision limits are not.
- The proposed layout is hand-modelled: the engine ignores the flat fields today, and the proposed A2 arm
  omits the original-value fallback.
- B2's computed bounds are a day-precision Spark expression, not `date_range`, and the comparison is the
  `TemporalComparator` shape, not the engine.
- The pruning rule does not look through `Cast`, which is why a strip of `__` fields over the engine's quantity
  output still reads the canonical form. Adding a `Cast` case, or the engine emitting the System quantity
  without a `cast`, would change that; not tried.
- Part A has no repeating quantity (`Observation.component.valueQuantity`); the rule's value there is not
  measured here.

## Reproduction

`specs/001-parquet-on-fhir/evidence/scripts/annotations-design/quantity-root/run.sh [prepare|a|a3|b|c|all]` builds
the rule and the prototypes with the Scala 2.13 compiler from `~/.m2`, generates 200,000 Observations
(`gen_observations.py`), writes the variants (`Prepare.scala`), and runs `PartA`, `PartA3`, `PartB` and `PartC`.
It needs the pushdown evidence's `work_big` table (`pushdown/go.sh PushdownPrepare.java` on
`gen_patients.py 1200000`). Output lands in `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/quantity-root/part*.out`.
Files: `Prototypes.scala` (`columnOrElse`, `ColumnsStruct`, `ResolveOrNone`), `Common.scala`, `env.sh`, `build.sh`,
`jrun.sh`, `Schema.scala` (prints the encoded schema).
