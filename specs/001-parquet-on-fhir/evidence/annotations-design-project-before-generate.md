# Does "project before generate" let stock Spark 4.3.0 prune the engine's views?

A measurement on the real view engine of the idea in
[annotations-design-spark-4-3-upstream-pruning.md](annotations-design-spark-4-3-upstream-pruning.md):
if the engine computes the `inline` input in a `Project` directly under the `Generate`, Spark 4.3.0's
own schema pruning might reach it, with no custom rule. The engine is not edited. Each view's
analysed plan is rewritten in a probe so that every `Generate` reads a column computed by a
`Project` beneath it, then run.

**Result: yes for most views, and the rest are bounded by two expressions, not by the `Generate`.**
Of 26 views on Spark 4.3.0 with the rule off, the rewritten plan:

- **A, prunes as well as our rule (18):** every `name`/`address` traversal, `forEach`,
  `forEachOrNull`, nested `forEach`, `unionAll`, `first()`, and a `where` that is a `first()` or a
  comparison on `period.start`. The 4 singular-meta views are counted here but have nothing to
  prune (all three plans read the same), so the real count is 14 of 22.
- **B, prunes partly (4):** `name.period.start`, `forEach name / period.start`,
  `address.period.start`, and the multi-column view with a `where`. The first three read the
  `period._start` annotation group that the rule drops; the fourth reads `name` whole because of its
  `where`.
- **C, still whole (4):** `name.exists()`, `name.count()`, and the two `where` views that use
  `exists(criteria)` or `name.where(...).exists()`. The blocker is `size(filter(name, ...))`, a
  presence-only use that demands the whole element. Our rule narrows these to `name[].use`.
- **Results are equal in all 26 views, on both versions.** Full rows, sorted, and `except` in both
  directions; output schemas are equal.
- **The optimiser keeps the `Project`.** In all 26 optimised plans the one `Generate` has its input
  as an attribute and a `Project` as its child. Nothing folds it back.
- **All 26 views had 4.3.0 reading `name` or `address` whole before the rewrite** (the singular-meta
  four read the same before and after). On Spark 4.0.2 the rewrite prunes nothing, as expected, and
  results are equal.

## Results

Reads are the leaves Parquet reads (the `ReadSchema` of `FileSourceScanExec`), in the compact form
of `CaseProbe`. A bare name is read entirely. "Rule on" is the existing `LambdaPruning.scala`
(unchanged copy) on the original plan. The class compares the rewritten, rule-off read with the
rule-on read: A if equal, C if equal to the original rule-off read, otherwise B. The blockers are
read off the optimised plans (`blockers-4.3.log`), not isolated by a further run.

The 22 name and address views are on the 22-patient set of the engine probe (20 patients with
names, addresses, periods and annotations, plus two edge patients); the singular-meta views are on
its 2000-row `small` set.

### Spark 4.3.0

| View | Original, rule off | Rewritten, rule off | Original, rule on | Class | Blocker | Results equal |
| --- | --- | --- | --- | --- | --- | --- |
| `name.family` | name | name[].family | name[].family | A |  | yes (22 rows) |
| `name.given` | name | name[].given | name[].given | A |  | yes (22 rows) |
| `name.period.start` | name | name[].period.start, name[].period._start | name[].period.start | B | `period._start` is read: the traversal builds `struct(value, start, range, named_struct(__start, _start.__start, __end, _start.__end))` and only `.value` is used, which stock Spark cannot trim | yes (22 rows) |
| `name.where(period.start > @1990).family` | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A |  | yes (22 rows) |
| `name.where(use = 'nickname').given` | name | name[].use, name[].given | name[].use, name[].given | A |  | yes (22 rows) |
| `name.exists()` | name | name | name[].use | C | `size(filter(name, x -> x is not null))`: a presence-only use demands the whole element | yes (22 rows) |
| `name.count()` | name | name | name[].use | C | `size(filter(name, x -> x is not null))`: a presence-only use demands the whole element | yes (22 rows) |
| `address.period.start` | address | address[].period | address[].period.start | B | `period._start` is read: the traversal builds `struct(value, start, range, named_struct(__start, _start.__start, __end, _start.__end))` and only `.value` is used, which stock Spark cannot trim (address.period holds only start and _start here) | yes (22 rows) |
| `address.city \| name.family` | name, address | name[].family, address[].city | name[].family, address[].city | A |  | yes (22 rows) |
| forEach name / `family` | name | name[].family | name[].family | A |  | yes (41 rows) |
| forEach name / `period.start` | name | name[].period.start, name[].period._start | name[].period.start | B | `period._start` is read: the traversal builds `struct(value, start, range, named_struct(__start, _start.__start, __end, _start.__end))` and only `.value` is used, which stock Spark cannot trim | yes (41 rows) |
| `name.first().family` | name | name[].family | name[].family | A |  | yes (22 rows) |
| `name.where(family.exists()).given` | name | name[].family, name[].given | name[].family, name[].given | A |  | yes (22 rows) |
| multi-column view with where and unionAll | id, name, birthDate, address | id, name, birthDate, address[].city | id, name[].use, name[].family, birthDate, address[].city | B | name read whole: the where is `size(filter(name, ...))` in a Filter (as `name.exists()`); address is pruned | yes (60 rows) |
| nested forEach name / forEach given | name | name[].family, name[].given | name[].family, name[].given | A |  | yes (60 rows) |
| forEachOrNull name / family | name | name[].family | name[].family | A |  | yes (42 rows) |
| forEach name / family, where on annotation-free leaf (use) | name | name | name[].use, name[].family | C | where is `size(filter(name, ...))` in a Filter | yes (40 rows) |
| forEach name / family, where with exists(criteria) | name | name | name[].use, name[].family | C | where is `size(filter(name, ...))` in a Filter | yes (40 rows) |
| forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name[].family | name[].family | A |  | yes (2 rows) |
| unionAll of two forEach (name family, address city) | name, address | name[].family, address[].city | name[].family, address[].city | A |  | yes (61 rows) |
| forEach name / family, where on period.start (annotated leaf) | name | name[].family, name[].period.start, name[].period._start | name[].family, name[].period.start, name[].period._start | A |  | yes (12 rows) |
| forEach name / period.start compared in column | name | name[].period.start, name[].period._start | name[].period.start, name[].period._start | A |  | yes (41 rows) |
| singular-meta small: `birthDate` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | A | nothing to prune: the read is the same in all three | yes (2000 rows) |
| singular-meta small: `birthDate.where($this > @1990)` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | A | nothing to prune: the read is the same in all three | yes (2000 rows) |
| singular-meta small: `birthDate \| deceasedDateTime` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A | nothing to prune: the read is the same in all three | yes (2000 rows) |
| singular-meta small: `(birthDate \| deceasedDateTime).where($this > @1990).exists()` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | A | nothing to prune: the read is the same in all three | yes (2000 rows) |

- A: 18
- B: 4
- C: 4

### Spark 4.0.2

| View | Original, rule off | Rewritten, rule off | Original, rule on | Results equal |
| --- | --- | --- | --- | --- |
| `name.family` | name | name | name[].family | yes (22 rows) |
| `name.given` | name | name | name[].given | yes (22 rows) |
| `name.period.start` | name | name | name[].period.start | yes (22 rows) |
| `name.where(period.start > @1990).family` | name | name | name[].family, name[].period.start, name[].period._start | yes (22 rows) |
| `name.where(use = 'nickname').given` | name | name | name[].use, name[].given | yes (22 rows) |
| `name.exists()` | name | name | name[].use | yes (22 rows) |
| `name.count()` | name | name | name[].use | yes (22 rows) |
| `address.period.start` | address | address | address[].period.start | yes (22 rows) |
| `address.city \| name.family` | name, address | name, address | name[].family, address[].city | yes (22 rows) |
| forEach name / `family` | name | name | name[].family | yes (41 rows) |
| forEach name / `period.start` | name | name | name[].period.start | yes (41 rows) |
| `name.first().family` | name | name | name[].family | yes (22 rows) |
| `name.where(family.exists()).given` | name | name | name[].family, name[].given | yes (22 rows) |
| multi-column view with where and unionAll | id, name, birthDate, address | id, name, birthDate, address | id, name[].use, name[].family, birthDate, address[].city | yes (60 rows) |
| nested forEach name / forEach given | name | name | name[].family, name[].given | yes (60 rows) |
| forEachOrNull name / family | name | name | name[].family | yes (42 rows) |
| forEach name / family, where on annotation-free leaf (use) | name | name | name[].use, name[].family | yes (40 rows) |
| forEach name / family, where with exists(criteria) | name | name | name[].use, name[].family | yes (40 rows) |
| forEach name / family, where first().family = 'F1' (annotation-free leaf) | name | name | name[].family | yes (2 rows) |
| unionAll of two forEach (name family, address city) | name, address | name, address | name[].family, address[].city | yes (61 rows) |
| forEach name / family, where on period.start (annotated leaf) | name | name | name[].family, name[].period.start, name[].period._start | yes (12 rows) |
| forEach name / period.start compared in column | name | name | name[].period.start, name[].period._start | yes (41 rows) |
| singular-meta small: `birthDate` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | yes (2000 rows) |
| singular-meta small: `birthDate.where($this > @1990)` | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | birthDate, _birthDate.__start, _birthDate.__end | yes (2000 rows) |
| singular-meta small: `birthDate \| deceasedDateTime` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | yes (2000 rows) |
| singular-meta small: `(birthDate \| deceasedDateTime).where($this > @1990).exists()` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDateTime.__start, _deceasedDateTime.__end | yes (2000 rows) |

On 4.0.2 the rewrite changes nothing (every `name`/`address` view reads the same whole column as
the original), results are equal, and the `Project` is kept there too. The rule-on column shows the
rule still works on 4.0.2.

## What the rewritten plan looks like

The engine's plan for `forEach name / family` is `Project(Generate(inline(<expr>), Relation))`. The
rewrite gives `Project(Generate(inline(nm_0), Project([..., <expr> AS nm_0], Relation)))`. After
optimisation the child `Project` stays, the other columns are pruned from it, and Spark adds
`InferFiltersFromGenerate` (`size(<expr>) > 0 AND isnotnull(<expr>)`) between the `Project` and the
scan. That `Filter` repeats the whole expression over the scan, and pruning still reaches the leaves
for the A views, so it does no harm there.

Every view the engine builds has exactly one `Generate`: a nested `forEach` and a `unionAll` are
folded into one expression (`flatten`/`transform`, `concat`). The stacked-`Generate` case, with a
`Project` under each, is therefore handled by the rewrite (`transformUp`, one `Project` per
`Generate`) but was not exercised by any view.

## Where the engine change would go

One site: `Projection.execute` in
`fhirpath/src/main/java/au/csiro/pathling/projection/Projection.java`, which does
`filteredResult.select(inline(projectionResult.getResultColumn()))` (line 137). No other
`inline`, `inline_outer` or `explode` in the core modules is on the view path (`BundleResource` and
`FhirXmlReader` use the Dataset `explode` for bundles). The change is to select the result column
under an alias first, `filteredResult.select(resultColumn.as("nm"))`, then
`.select(inline(col("nm")))`: about three lines, with the column-name lookup in lines 129 to 134
reading the first select unchanged. `renderColumn`'s metadata alias, which stops the optimiser
removing the outer `Project`, is unaffected. The `where` filter stays a `Filter` above the scan, so
the C and B views that are blocked by a `where` stay blocked unless the filter is also moved.

The rewrite in the probe is equivalent to this change for these views, with one difference: the
probe's `Project` also carries every child column, then drops it from the `Generate` through
`unrequiredChildIndex`. The engine change would carry only the one column, which is simpler and
reads the same.

## What stays for the rule

- `size(filter(name, ...))` over a whole element, which is what `exists()`, `count()` and a
  `where` on `exists` produce (C, and the `where` part of the B view).
- The unused `range` field of the `struct(value, start, range, ...)` built for each dateTime
  (`period.start`): the rule reads `period.start` alone; stock Spark reads `period._start` too.
  This is the one place the engine emits primitive metadata today, and it is small (two leaves
  of a group that holds only `__start` and `__end` here).

Both could be addressed in the engine instead: the first by testing emptiness without
`size`-over-`filter` where the element is not needed, the second by not building `range` unless a
column asks for it. This was not tried.

## Method

- `PlanRewrite.scala` (in package `org.apache.spark.sql` to reach `classic.Dataset.ofRows`):
  `transformUp` over the analysed plan; every `Generate` whose generator is `Inline`, `Explode` or
  `PosExplode` and whose input is not an `Attribute` gets `Project(child.output :+ Alias(input,
  "nm_i"), child)` beneath it, the generator is rebuilt on the alias attribute, and the new column is
  added to `unrequiredChildIndex`. The result is run with `classic.Dataset.ofRows`.
- `ProbeMain.java` builds each view through the library API (`QueryableDataSource.view(...)`, as
  `EngineProbe` does), runs the original with `LambdaPruning` uninstalled, the rewritten plan with it
  uninstalled, and the original with it installed. For each it reads `ReadSchema` from the executed
  plan and collects the sorted rows. Equality: sorted row strings equal across all three, output
  schemas equal, and `except` in both directions empty between original and rewritten.
- The optimiser check is `optimisedGenerates`: for every `Generate` in the optimised plan, the class
  of the input expression and of the child.
- Data: `gen_patients.py` (unchanged copy) encoded by the library to Parquet on FHIR. The singular-meta
  set is the one of the singular-meta-pruning spike, read only. Local mode, two cores, adaptive
  execution off, `spark.sql.maxMetadataStringLength` 100000.
- Classpath: as `engine.sh`. For 4.3.0 the 4.3.0 jars first, the third-party classpath of the 4.0.2
  runs without its Spark jars, and the Pathling 10.0.0-SNAPSHOT jars built against 4.0.2. For 4.0.2
  the 4.0.2 third-party classpath and the same jars. `PlanRewrite.scala` was compiled separately
  against each (`build.sh 4.3`, `build.sh 4.0`).
- Scripts in `scripts/annotations-design/project-before-generate/`: `env.sh`, `build.sh`, `run.sh`,
  `shape.sh` and `PlanShape.java` (print the plans of one view), `blockers.sh`, `make_tables.py`.
  Logs and data in `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/project-before-generate/`
  (`logs/probe-4.3.log`, `logs/probe-4.0.log`, `logs/tables.md`, `blockers-4.3.log`).

## Caveats

- The engine was not changed; the probe rewrites its analysed plan. A real change builds the same
  plan for these views, but the analysis that produces it differs (attribute ids, names), so a
  difference in a corner case is possible.
- Two cores, tiny data: read schemas are the claim, not timings.
- Class and blocker are by comparison of read schemas and inspection of the optimised plans; no
  blocker was isolated by removing the expression.
- `last()` is not supported by the engine, so a `where` on a leaf uses `first()`.
- The pruning rule's edge-case coverage (null elements, an element present only through its
  extension) is not part of this run; the 22-row set has two edge patients only.

## Versions

Spark 4.3.0 (Scala 2.13.18 at runtime) and 4.0.2, Java 21.0.3, Pathling 10.0.0-SNAPSHOT jars built
against 4.0.2.
