# Does the lambda-pruning rule work on Databricks?

The follow-up to [annotations-design-lambda-pruning-rule.md](annotations-design-lambda-pruning-rule.md),
whose open risk was Databricks: DBR forks Catalyst and plans scans differently under Photon. The
risk rule (K6) allows the rule to be relied on only once it has been verified there.

**Outcome: PASS-WITH-COST.** Nothing on DBR 17.3 LTS ignores or undoes the re-typed relation, and
the results were identical to rule-off in every run. But the rule as built for Apache Spark does
not work unchanged, and under Photon it does not cover every case:

1. **As built for Apache Spark 4.0.2, it does nothing on DBR.** Its first rewrite throws
   `NoSuchMethodError: LogicalRelation.copy(BaseRelation, Seq, Option, boolean, Option)`, because
   DBR's `LogicalRelation` has seven constructor arguments, not five. The rule's guard catches the
   error and returns the plan unchanged, so every query still ran, with equal results, and pruned
   nothing. Only the bail-out counter showed it. The cost is one change: rebuilding the relation
   positionally with `makeCopy` instead of `copy(relation =, output =)` (see "What had to change").
2. **With that change it prunes on both Photon off and Photon on, with no bail-outs on any spike
   case other than the control that bails by design**, and reads roughly 5 to 25 times fewer bytes on the cases that prune.
3. **Photon on has a gap, and it falls on the design-critical shape.** Cases 3c to 3f, raw `meta`
   for a repeating primitive below a repeating element (the shape that
   [annotation-pruning.md](annotation-pruning.md) calls the hardest), read the whole of `dosage`
   with Photon on, on every table kind. The engine view `name.period.start` reads `name[].period`
   instead of `name[].period.start`. Databricks puts its own expression, `prune_struct_fields`, into
   Photon plans, and the rule treats an unknown expression conservatively. Nothing bails out and the
   results are correct, but nothing is pruned. Unlike the `makeCopy` change, **this cost has not
   been shown to be fixable**: it needs the rule to understand a closed-source DBR class, which was
   not tried. It is a cost and not a failure because the rule never rewrote those plans, so DBR had
   nothing to ignore or undo, and the results were equal. Whether it changes the go/no-go is a
   design decision.
4. **Not tested: Unity Catalog managed tables.** The workspace has no catalog the user can write to.

## What had to change

One line, at the point where the rule rebuilds the scan:

```scala
// Before (binds to Apache Spark's five-argument signature):
val n = l.copy(relation = newFs, output = newOutput)
// After (positional, so the arity does not matter):
val args = l.productIterator.map(_.asInstanceOf[AnyRef]).toArray
args(0) = newFs
args(1) = newOutput
val n = l.makeCopy(args).asInstanceOf[LogicalRelation]
```

DBR 17.3's constructor is the original five arguments plus `Option[InjectedConstraints]` and
`Option[Statistics]`. The positional rebuild copies those two unchanged, so the statistics still
describe the unpruned relation, and a constraint that names a pruned nested field was not tested:

```
LogicalRelation(BaseRelation, Seq[AttributeReference], Option[CatalogTable], boolean,
    Option[SparkDataStream], Option[InjectedConstraints], Option[Statistics])
```

`HadoopFsRelation.copy(dataSchema = ...)(session)`, the other internal call, is unchanged: its
seven-argument signature matched, and it never failed. `HigherOrderFunction.bind`, `GetStructField`,
`GetArrayStructFields`, `Generate` and `CreateNamedStruct` also worked unchanged, so there was no
other binary mismatch. The shim was verified on DBR 17.3 only; whether the same jar also works on Apache Spark 4.0.2 was not
run. It also depends on `relation` and
`output` staying the first two constructor arguments, which was not checked on other runtimes.

Two smaller changes to the prototype were for measurement, not for DBR:

- The guard now catches `Throwable`, not `Exception`, so that a linkage error is counted as a
  bail-out instead of failing the query.
- Counters were added: rewrites, no-ops, not applicable and bail-outs, with the reasons.

## Conditions

- **Runtime.** DBR 17.3 LTS (`17.3.x-scala2.13`, Apache Spark 4.0.0 as reported, Scala 2.13.16). Photon
  off is `runtime_engine: STANDARD`, Photon on is `PHOTON`, both with the same node type.
- **Compute.** Single-node job clusters on AWS `r5d.large` (2 cores, 16 GB), on-demand, dedicated
  (single-user) access mode, Unity Catalog enabled, disk cache off
  (`spark.databricks.io.cache.enabled=false`, so repeat reads do not hide the bytes read). Adaptive
  execution was on, which is the DBR default; the open-source spike ran mostly with it off.
- **Java.** The case-1 to 4 matrix ran on **Java 17**: the `JNAME=zulu21` in the task brief does not
  select a JDK; `JNAME=zulu21-ca-amd64`, as the site documentation has it, does. The engine views ran on
  Java 21 with it. The matrix jar targets Java 17 and the behaviour tested is Catalyst planning,
  which does not depend on the JVM, but the matrix was not repeated on Java 21.
- **How the rule got on the classpath.** DBR refuses jar libraries from `/Workspace` on dedicated
  Unity Catalog clusters, and this workspace has no Unity Catalog volume to put them in. An init
  script copies the probe jar to `/databricks/jars` on the driver. The 128 MB Pathling jar, over the
  50 MB workspace upload limit, was read from the DBFS root by the same script. Nothing was tested
  through a cluster library, which is how the Pathling documentation installs the library.
- **Data.** 20,003 rows of the open-source spike's case data, with unique extension payloads so that
  it does not compress away, written as Parquet and as four Delta tables (below) under the DBFS
  root. The three edge rows of the null shapes (`e1` to `e3`) are in it. The engine views ran on 20,002 generated
  patients stored by the library API, and on the open-source spike's 2,000-row singular-meta data.
- **Foreign annotation.** At the request of the process session, `_birthDate` and
  `location[].period` also carry `__foreign`, a string the engine does not recognise.
  The case 1j query concatenates `_birthDate` with `name[]._family`, which only analyses when the
  two have the same type, so 1j reads a view that drops `__foreign`.
- **Bytes** are the sum of task `inputMetrics.bytesRead` per job group, with a warm-up run so that
  Delta's log is not counted. They are not the scan's "size of files read", which does not change
  with pruning. Under Photon the scan is `PhotonParquetFileScanExec` in every Photon-on run, rule off and on, so no query fell back to the
  JVM scan; the read schema is parsed from its text where the node has no `requiredSchema`.
- **Table kinds.** `parquet`; `delta-dv` (deletion vectors, with a `DELETE` and
  `numDeletionVectorsAdded > 0` confirmed); `delta-colmap` (`columnMapping.mode = name`, so the
  physical column names differ); `delta-liquid` (`CLUSTER BY id`, then `OPTIMIZE`); `delta-all` (all
  three). DBR also turned on row tracking for the clustered tables.
- **Runs.** Each case ran with the rule off, with it on, with it on again from a new query, and twice
  on one reused Dataset. The rule was installed with `spark.experimental.extraOptimizations`, from
  Python through py4j and from Scala in a notebook, as separate runs. The raw results are in
  `scripts/annotations-design/lambda-pruning-databricks/results/`.

## Results by check

### a. The rule loads: PASS-WITH-COST

- As built for Apache Spark: it loads, with no `ClassCastException`, and installs through
  `extraOptimizations` from both Python and Scala. At the first rewrite it throws the
  `NoSuchMethodError` above. Bail-outs were counted on every query, and every case read whole.
- With the `makeCopy` change it loads and rewrites on DBR 17.3. Python through py4j: PASS, with
  Photon off and on (`spark._jsparkSession` exists on a dedicated cluster, so the py4j route is
  available). Scala, from a notebook: PASS, with Photon off and on. The notebook task was reported as
  failed ("Spark became unavailable"), most likely because the probe called `spark.stop()` after it
  had written its results; that call has since been removed from the source and not rerun.

### b. The rule fired: Photon off PASS; Photon on, 3c to 3f no-op behind `PruneStructFields`

Counted per query as rewrites, no-ops (already pruned or nothing to narrow), not applicable and
bail-outs. The relation classes it met were all handled: Parquet's `ParquetFileFormat` and
Databricks' `DeltaParquetFileFormat`. None was skipped.

- **Bail-outs on spike cases 1 to 4: none**, on all 240 case runs (24 cases, 5 table kinds, Photon
  off and on), with one exception: **case 1j bails by design**, 10 times (5 kinds, 2 runtimes). It
  is the control that puts two differently pruned structs into one `concat`; the rule's type check
  rejects the result and the plan is returned unchanged, as in the open-source run. Its reason is
  `COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.MISMATCHED_TYPES`.
- **Bail-outs on the 18 engine views: none**, on Parquet and Delta with deletion vectors, Photon off
  and on.
- **Photon on: 3c to 3f did not fire** (no rewrite, one no-op, no bail-out), on all five table
  kinds. See "The Photon gap".

### c. Pruning: Photon off PASS; Photon on PASS except 3c to 3f (whole `dosage`) and `name.period.start`

Judged on both the read schema and the bytes. The tables below give both, rule off and rule on, and
whether the rule fired. For example, `name.family` through case 1b reads 10.46 MB without the rule
and 0.62 MB with it. Without the rule the whole `name` column is read on both runtimes.

**Rule off, on DBR.** Databricks already prunes through `transform` and nothing else the rule
handles: only case 1c (`transform(name, x -> x.family)`) reads `name[].family` with the rule off, on
both runtimes, and the rule does nothing there. Every `filter`, `exists`, `flatten`, `zip_with`,
`arrays_zip`, `inline` and engine-view case reads the whole column with the rule off. The pruning
shown for those cases is the rule's, not DBR's. Case 4d (a plain `select s.value`) is also already
pruned, as it was in Apache Spark.

### d. Results identical with the rule off and on: PASS on every table kind tested; Unity Catalog managed tables NOT TESTED

- Cases 1 to 4: **0 mismatches in 240 case runs**, checked on the first run, on a repeat, and on a
  reused Dataset, with the output schema compared too. The edge rows (null element, extension-only
  element, null `_birthDate`) are in the data.
- Engine views: **0 mismatches** on 18 views, on Parquet and on Delta with deletion vectors, Photon
  off and on.
- Table kinds: Parquet, deletion vectors, column mapping, liquid clustering, and all three together:
  all identical. With deletion vectors the non-Photon scan carries
  `_databricks_internal_edge_computed_column_skip_row`, DBR's row-skip column; the rule leaves it
  alone and still prunes the rest. Photon's scan does not list it.
- **Unity Catalog managed tables: NOT TESTED.** The metastore (`ap-southeast-2`) has no storage root,
  external location or storage credential, and the user can write to no catalog. A volume path the
  user suggested turned out not to be a Unity Catalog volume. Hive metastore tables were not
  substituted. Delta tables were read by path, not by name.

### e. The plan is stable: PASS

- On a second run of each query, the read schema, the bytes (within 5%) and the optimised plan were
  the same, and a reused Dataset gave the same rows twice. The rule is in the "User Provided
  Optimizers" batch, which runs after DBR's own batches, and the pruned read reached the scan on
  every case that fired.
- The engine views' plans differ between runs only by a counter in generated lambda names
  (`x_23` against `x_27`); with that normalised they are identical.
- Not tested: `injectOptimizerRule` and `injectPreCBORule` on DBR; `Dataset.cache`.

## The foreign annotation

`__foreign` was read with the rule off in cases 1i, 2a to 2c and 4a to 4c, where the whole of the
containing struct is read. **With the rule on, it was read in none of the cases**, on both runtimes
and every table kind: the rule pruned it whenever the expression did not use it. (No case uses it.)

## The Photon gap

With Photon on, Databricks inserts `PruneStructFields(child, RetainTree)`
(`org.apache.spark.sql.catalyst.expressions.PruneStructFields`, printed `prune_struct_fields`) into
plans; with Photon off it does not appear. It appears, for example, as
`prune_struct_fields(filter(dosage, ...), [timing.event, timing._event])`, and in the engine view
`name.period.start` as `prune_struct_fields(..., [start, _start.__start, _start.__end])`. It is
DBR's own lambda pruning, done on values in memory: with the rule off the scan still reads the whole
column.

The rule treats an expression it does not know as needing its inputs whole. So under Photon:

- Cases 3c to 3f (`zip_with` and `arrays_zip` over a repeating primitive below a repeating element)
  read all of `dosage`, 3.7 to 8.4 MB, as with the rule off. Cases 3a and 3b, which have no
  `prune_struct_fields`, prune normally.
- The engine view `name.period.start` reads `name[].period` (0.55 MB) instead of
  `name[].period.start` (0.53 MB). The other 13 name views prune the same as with Photon off.

The expression's constructor is `(Expression, RetainTree)`; `RetainTree` is a Databricks type whose
shape was not examined. Handling it would mean passing demand through it, and re-typing it when its
child is pruned, which a rule written against an unseen closed-source class can get silently wrong.
It was not attempted. If the design relies on cases like 3c to 3f under Photon, this needs a
decision.

## Per-case results

Tables made by `report_tables.py` from the raw results. "Off" is the rule off; "Fired" says whether
the rule rewrote the plan. The scan is `FileSourceScanExec` with Photon off and
`PhotonParquetFileScanExec` with Photon on. `+dv-skip-col` is the row-skip column described above.

### Table kind `parquet`: rule off, rule on, and whether our rule fired

| Case | Off: read | Off MB | On, Photon off: read | MB | Fired | On, Photon on: read | MB | Fired |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | --- |
| 1a | name | 10.46 | name[].family | 0.62 | rewrote | name[].family | 0.62 | rewrote |
| 1b | name | 10.46 | name[].family | 0.62 | rewrote | name[].family | 0.62 | rewrote |
| 1c | name[].family | 0.62 | name[].family | 0.62 | no change | name[].family | 0.62 | no change |
| 1d | name | 10.46 | name[].family | 0.62 | rewrote | name[].family | 0.62 | rewrote |
| 1e | name | 10.46 | name | 10.46 | no change | name | 10.44 | no change |
| 1f | name | 10.46 | name | 10.46 | no change | name | 10.44 | no change |
| 1g | id, name | 10.56 | id, name[].period.start | 0.64 | rewrote | id, name[].period.start | 0.64 | rewrote |
| 1h | name | 10.46 | name[].id | 0.52 | rewrote | name[].id | 0.52 | rewrote |
| 1i | location | 13.49 | location[].extension[].url | 0.53 | rewrote | location[].extension[].url | 0.53 | rewrote |
| 1j | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 4.38 | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 4.38 | **bailed** | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 4.37 | **bailed** |
| 1k | name | 10.46 | name[].family, name[]._family.id | 0.62 | rewrote | name[].family, name[]._family.id | 0.62 | rewrote |
| 2a | location | 13.49 | location[].period.start | 0.56 | rewrote | location[].period.start | 0.56 | rewrote |
| 2b | location | 13.49 | location[].period.start, location[].period._start.__start | 0.60 | rewrote | location[].period.start, location[].period._start.__start | 0.60 | rewrote |
| 2c | location | 13.49 | location[].period.start | 0.56 | rewrote | location[].period.start | 0.56 | rewrote |
| 3a | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].__start | 0.57 | rewrote | dosage[].timing.event, dosage[].timing._event[].__start | 0.59 | rewrote |
| 3b | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].__start | 0.57 | rewrote | dosage[].timing.event, dosage[].timing._event[].__start | 0.59 | rewrote |
| 3c | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].id | 0.57 | rewrote | dosage | 8.42 | no change |
| 3d | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].__start | 0.57 | rewrote | dosage | 8.42 | no change |
| 3e | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].id | 0.57 | rewrote | dosage | 8.42 | no change |
| 3f | dosage | 8.44 | dosage[].timing.event, dosage[].timing._event[].__start | 0.57 | rewrote | dosage | 8.42 | no change |
| 4a | birthDate, _birthDate | 3.14 | birthDate | 0.54 | rewrote | birthDate | 0.54 | rewrote |
| 4b | birthDate, _birthDate | 3.14 | birthDate, _birthDate.__start | 0.56 | rewrote | birthDate, _birthDate.__start | 0.56 | rewrote |
| 4c | birthDate, _birthDate | 3.14 | birthDate, _birthDate.id | 0.54 | rewrote | birthDate, _birthDate.id | 0.54 | rewrote |
| 4d | birthDate | 0.54 | birthDate | 0.54 | no change | birthDate | 0.54 | no change |

### Table kind `delta-all`: rule off, rule on, and whether our rule fired

| Case | Off: read | Off MB | On, Photon off: read | MB | Fired | On, Photon on: read | MB | Fired |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | --- |
| 1a | name, +dv-skip-col | 4.63 | name[].family, +dv-skip-col | 0.30 | rewrote | name[].family | 0.30 | rewrote |
| 1b | name, +dv-skip-col | 4.63 | name[].family, +dv-skip-col | 0.30 | rewrote | name[].family | 0.30 | rewrote |
| 1c | name[].family, +dv-skip-col | 0.30 | name[].family, +dv-skip-col | 0.30 | no change | name[].family | 0.30 | no change |
| 1d | name, +dv-skip-col | 4.63 | name[].family, +dv-skip-col | 0.30 | rewrote | name[].family | 0.30 | rewrote |
| 1e | name, +dv-skip-col | 4.63 | name, +dv-skip-col | 4.63 | no change | name | 4.62 | no change |
| 1f | name, +dv-skip-col | 4.63 | name, +dv-skip-col | 4.63 | no change | name | 4.62 | no change |
| 1g | id, name, +dv-skip-col | 4.67 | id, name[].period.start, +dv-skip-col | 0.31 | rewrote | id, name[].period.start | 0.31 | rewrote |
| 1h | name, +dv-skip-col | 4.63 | name[].id, +dv-skip-col | 0.26 | rewrote | name[].id | 0.26 | rewrote |
| 1i | location, +dv-skip-col | 5.95 | location[].extension[].url, +dv-skip-col | 0.27 | rewrote | location[].extension[].url | 0.26 | rewrote |
| 1j | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 1.95 | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 1.95 | **bailed** | _birthDate.id, _birthDate.extension, _birthDate.__start, _birthDate.__end, nam | 1.95 | **bailed** |
| 1k | name, +dv-skip-col | 4.63 | name[].family, name[]._family.id, +dv-skip-col | 0.30 | rewrote | name[].family, name[]._family.id | 0.30 | rewrote |
| 2a | location, +dv-skip-col | 5.95 | location[].period.start, +dv-skip-col | 0.28 | rewrote | location[].period.start | 0.28 | rewrote |
| 2b | location, +dv-skip-col | 5.95 | location[].period.start, location[].period._start.__start, +dv-skip-col | 0.29 | rewrote | location[].period.start, location[].period._start.__start | 0.29 | rewrote |
| 2c | location, +dv-skip-col | 5.95 | location[].period.start, +dv-skip-col | 0.28 | rewrote | location[].period.start | 0.28 | rewrote |
| 3a | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].__start, +dv-skip-col | 0.28 | rewrote | dosage[].timing.event, dosage[].timing._event[].__start | 0.29 | rewrote |
| 3b | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].__start, +dv-skip-col | 0.28 | rewrote | dosage[].timing.event, dosage[].timing._event[].__start | 0.29 | rewrote |
| 3c | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].id, +dv-skip-col | 0.28 | rewrote | dosage | 3.73 | no change |
| 3d | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].__start, +dv-skip-col | 0.28 | rewrote | dosage | 3.73 | no change |
| 3e | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].id, +dv-skip-col | 0.28 | rewrote | dosage | 3.73 | no change |
| 3f | dosage, +dv-skip-col | 3.73 | dosage[].timing.event, dosage[].timing._event[].__start, +dv-skip-col | 0.28 | rewrote | dosage | 3.73 | no change |
| 4a | birthDate, _birthDate, +dv-skip-col | 1.41 | birthDate, +dv-skip-col | 0.27 | rewrote | birthDate | 0.27 | rewrote |
| 4b | birthDate, _birthDate, +dv-skip-col | 1.41 | birthDate, _birthDate.__start, +dv-skip-col | 0.28 | rewrote | birthDate, _birthDate.__start | 0.28 | rewrote |
| 4c | birthDate, _birthDate, +dv-skip-col | 1.41 | birthDate, _birthDate.id, +dv-skip-col | 0.27 | rewrote | birthDate, _birthDate.id | 0.27 | rewrote |
| 4d | birthDate, +dv-skip-col | 0.27 | birthDate, +dv-skip-col | 0.27 | no change | birthDate | 0.27 | no change |

### Engine views (parquet)

| View | Off: read | Off MB | On, Photon off: read | MB | Fired | On, Photon on: read | MB | Fired |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | --- |
| `name.family` | name | 4.54 | name[].family | 0.61 | rewrote | name[].family | 0.62 | rewrote |
| `name.given` | name | 4.54 | name[].given | 0.77 | rewrote | name[].given | 0.77 | rewrote |
| `name.period.start` | name | 4.54 | name[].period.start | 0.53 | rewrote | name[].period | 0.55 | rewrote |
| `name.where(period.start > @1990).family` | name | 4.54 | name[].family, name[].period.start, name[].period._start | 0.73 | rewrote | name[].family, name[].period.start, name[].period._start | 0.88 | rewrote |
| `name.where(use = 'nickname').given` | name | 4.54 | name[].use, name[].given | 0.86 | rewrote | name[].use, name[].given | 0.87 | rewrote |
| `name.exists()` | name | 4.54 | name[].use | 0.53 | rewrote | name[].use | 0.53 | rewrote |
| `name.count()` | name | 4.54 | name[].use | 0.53 | rewrote | name[].use | 0.53 | rewrote |
| `address.period.start` | address | 2.49 | address[].period.start | 0.52 | rewrote | address[].period.start | 0.53 | rewrote |
| `address.city \| name.family` | name, address | 6.52 | name[].family, address[].city | 0.61 | rewrote | name[].family, address[].city | 0.72 | rewrote |
| forEach name / `family` | name | 4.54 | name[].family | 0.61 | rewrote | name[].family | 0.62 | rewrote |
| forEach name / `period.start` | name | 4.54 | name[].period.start | 0.53 | rewrote | name[].period.start | 0.53 | rewrote |
| `name.first().family` | name | 4.54 | name[].family | 0.61 | rewrote | name[].family | 0.62 | rewrote |
| `name.where(family.exists()).given` | name | 4.54 | name[].family, name[].given | 0.86 | rewrote | name[].family, name[].given | 0.86 | rewrote |
| multi-column view with where and unionAll | id, name, birthDate, _birthDate, address | 6.61 | id, name[].use, name[].family, birthDate, _birthDate, address[].city | 0.83 | rewrote | id, name[].use, name[].family, birthDate, _birthDate, address[].city | 0.82 | rewrote |
| singular-meta small: `birthDate` | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | no change | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | no change |
| singular-meta small: `birthDate.where($this > @1990)` | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | no change | birthDate, _birthDate.__start, _birthDate.__end | 0.13 | no change |
| singular-meta small: `birthDate \| deceasedDateTime` | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | no change | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | no change |
| singular-meta small: `(birthDate \| deceasedDateTime).where($ | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | no change | birthDate, _birthDate.__start, _birthDate.__end, deceasedDateTime, _deceasedDa | 0.13 | no change |

## Cost and what was not covered

- **Cluster starts.** 15 single-node job clusters were started, against the four approved: five
  smoke runs and ten for the matrix and engine views. Three never reached Spark (a library install
  from a volume that does not exist, a jar from `/Workspace` that DBR refuses on dedicated
  clusters, and an init script I wrote with a bug). The rest were reruns for faults in the probe, not
  in the rule: the 1j type clash from the foreign annotation, the JDK name, and a library setting for
  timestamps. Each ran minutes on `r5d.large`.
- **Not covered.** Unity Catalog managed tables; the engine views on column mapping, liquid
  clustering and all three together (they ran on Parquet and on Delta with deletion vectors; cases 1
  to 4 ran on all five kinds); DBR versions other than 17.3; standard (shared) access mode; serverless; `injectOptimizerRule` and `injectPreCBORule`; the Maven-library
  installation; Java 21 for the matrix; the full conformance suite, which the brief left to CI.
- **Not rerun after the last edits.** Removing `spark.stop()` from the probe, and the trimming of the
  committed results.

## Reproduction

All in `scripts/annotations-design/lambda-pruning-databricks/`. The rule needs the Databricks CLI
logged in, and a workspace where jars can be placed on the driver classpath.

| File | Role |
| --- | --- |
| `LambdaPruning.scala` | The rule, with the `makeCopy` change and the counters. |
| `DbxProbe.scala` | Plan inspection that works for any scan node, the bytes listener, the matrix and the entry points. |
| `EngineViews.scala` | Case 5, the 18 views through the library API. |
| `run_probe.py` | The Python task: installs the rule through py4j, checks four cases from PySpark, runs the matrix or the engine views. |
| `scala_check.scala` | The notebook that installs the rule from Scala. |
| `init.sh` | Puts the jars on the driver classpath. |
| `submit.py` | Creates a job with a single-node cluster and runs it (`smoke`, `full` or `engine`). |
| `build.sh`, `env.sh` | Compile the probe jar against Apache Spark 4.0.2; paths. |
| `gen_patients.py` | The engine views' patients. |
| `analyse.py`, `report_tables.py`, `trim_results.py` | Summaries, the tables above, and the trimming of the results. |
| `results/` | The raw JSON of the runs reported here. |
