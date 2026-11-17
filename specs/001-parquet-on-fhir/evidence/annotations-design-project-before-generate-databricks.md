# Does "project before generate" prune the engine's views on Databricks (DBR 17.3 LTS, Photon on)?

The Databricks counterpart of [annotations-design-project-before-generate.md](annotations-design-project-before-generate.md).
Setup as in [annotations-design-lambda-pruning-databricks.md](annotations-design-lambda-pruning-databricks.md):
DBR 17.3 LTS (Spark 4.0.0 fork, Scala 2.13.16), dedicated access mode, **Photon on**, one single-node
`r5d.large` job cluster, Java 21, disk cache off, Pathling 10.0.0-SNAPSHOT jars from an init script. **Our
rule is never installed** (`extraOptimizations` size 0, printed at the start of the run). One run, one
cluster, both table kinds.

**Result: compute-first prunes nothing on DBR 17.3 with Photon. Every name or address view still reads
its element column whole.** Of the 18 views, none of the 14 name/address views read less than the
original. The 4 singular-meta views read the same in all plans (nothing to prune), so they are class A
only vacuously.

- **Results are equal in all 18 views, on both table kinds.** Full rows sorted, `except` in both
  directions (both zero), row counts, and output schemas.
- **The rewrite applied every time.** Each view is rewritten three times (a warm-up and two measured
  runs) and has exactly one `Generate`, so Parquet counts 54 rewrites (18 x 3); the counter is cumulative in the session, so the
  Delta run's total of 108 is 54 more, with 0 no-ops and 0 failures. The two measured
  runs of each view report `[2, 0, 0]`. A failed rewrite would have been counted and would have raised.
- **The optimiser kept the `Project` under the `Generate`.** In every plan the `Generate` reads an
  attribute (`nm_0`) from a `Project`. DBR did not fold it back; it also did not push the pruning
  into it.
- **Photon read the same bytes as the original.** `name.family`: 4,521,838 bytes original and
  compute-first; our rule gave 620,521 in the earlier run. All 13 name/address views match their
  originals to the byte.
- **The blocker is the `filter` over the element array, not the `Generate`.** The expression under the
  `Project` is `coalesce(filter(filter(name, x -> isnotnull(x)).family, ...), [])`. DBR pruned through
  `transform` only in the earlier rule-off baseline (case 1c), and here the traversal is a `filter`,
  so the whole `name` is read. This is read off the plans and that baseline; I did not isolate it by
  removing the expression.
- **`InferFiltersFromGenerate` repeats the whole expression** in a `Filter` between the `Project` and
  the scan (`size(array(struct(...))) > 0`), and it becomes a Photon data filter. This is the same
  `Filter` as on 4.3.0 locally; it may be a second blocker, because it also demands the whole column.
  Not isolated.
- **`prune_struct_fields`** appears only for `name.period.start` (2 occurrences in the optimised plan
  and the executed plan, in the `Project` and in the `Filter`, as in the original plan's `Generate`).
  Everywhere else it is absent. It does not trim anything the original did not.

## Per view (Parquet; Delta with deletion vectors is identical)

Read is the Photon scan's `ReadSchema` in compact form. "Rule on" is from the earlier run
(`lambda-pruning-databricks/results/engine3-photon`, same data), not re-run. Class: A equals the rule-on
read, C equals the original read, B otherwise. Full table with bytes, `Project` kept and
`prune_struct_fields` counts: `project-before-generate-databricks/results/tables.md`.

| View | Original, rule off | Compute-first | Rule on (earlier) | Class | Bytes: orig / CF / rule on |
| --- | --- | --- | --- | --- | --- |
| `name.family` | name | name | name[].family | C | 4.52M / 4.52M / 0.62M |
| `name.given` | name | name | name[].given | C | 4.52M / 4.52M / 0.77M |
| `name.period.start` | name | name | name[].period | C | 4.52M / 4.52M / 0.55M |
| `name.where(period.start > @1990).family` | name | name | name[].family, period.start, period._start | C | 4.52M / 4.52M / 0.88M |
| `name.where(use = 'nickname').given` | name | name | name[].use, given | C | 4.52M / 4.52M / 0.87M |
| `name.exists()` | name | name | name[].use | C | 4.52M / 4.52M / 0.53M |
| `name.count()` | name | name | name[].use | C | 4.52M / 4.52M / 0.53M |
| `address.period.start` | address | address | address[].period.start | C | 2.49M / 2.49M / 0.53M |
| `address.city \| name.family` | name, address | name, address | name[].family, address[].city | C | 6.50M / 6.50M / 0.72M |
| forEach name / `family` | name | name | name[].family | C | 4.52M / 4.52M / 0.62M |
| forEach name / `period.start` | name | name | name[].period.start | C | 4.52M / 4.52M / 0.53M |
| `name.first().family` | name | name | name[].family | C | 4.52M / 4.52M / 0.62M |
| `name.where(family.exists()).given` | name | name | name[].family, given | C | 4.52M / 4.52M / 0.86M |
| multi-column view with where and unionAll | id, name, birthDate, **_birthDate**, address | id, name, birthDate, address | id, name[].use, name[].family, birthDate, _birthDate, address[].city | B | 6.59M / 6.59M / 0.82M |
| 4 singular-meta views | same in all three | same | same | A (vacuous) | 133,371 each |

Totals: C 13, B 1, A 4 (vacuous), on both Parquet and Delta with deletion vectors. The B view is B only
because the unused `_birthDate` is no longer read; `name` and `address` are still whole and the bytes
are identical to the original. The local 4.3.0 result was 14 of 22 as tight as the rule; here it is 0 of 14.

## Conditions and what was not covered

- **Parquet and Delta with deletion vectors only** (3 rows deleted; row counts 19,999 etc. reflect that).
  Column mapping, liquid clustering, and UC managed tables not run.
- **Photon only**, as asked. Photon off was not run, so whether DBR's non-Photon path behaves the same
  is unknown.
- **The rewrite is a plan rewrite in a probe**, as locally, not the engine change. It uses `makeCopy`
  by field name for `Generate` and `classic.Dataset.ofRows`, which worked on DBR 17.3 without a shim.
  A real engine change builds the plan through the Dataset API and could differ in attribute ids only.
- **Plan stability:** the read schema is identical on the repeat run in all 18 views, results equal on
  the repeat run. The optimised-plan text differed between two runs in 5 views per table kind
  (`forEach name / family`, `forEach name / period.start`, the multi-column view, and two singular-meta
  views); I normalise expression ids and lambda counters but did not investigate the rest. Reads and
  results were stable.
- 20,002 patients (19,999 after the delete), AQE on, one run per variant, so bytes are task
  `inputMetrics.bytesRead` totals, not timings. The rule-on column comes from the earlier run, which
  used the same ndjson input and the same views.
- Compute: one cluster, started once, a few minutes; no reruns. The job was deleted afterwards.

## Reproduction

Scripts in `scripts/annotations-design/project-before-generate-databricks/`:

| File | Purpose |
| --- | --- |
| `PbgRewrite.scala` | The rewrite for DBR (`makeCopy` by field name; counters: applied, nothing to do, failed). |
| `PbgViews.scala` | Per view: original, rewritten (twice), equality, plan facts, `prune_struct_fields` lines, bytes. Reuses `DbxProbe` and `EngineViews` from `lambda-pruning-databricks`. |
| `build.sh` | Compiles both against Spark 4.0.2 and `lpr.jar` into `pbg.jar`. |
| `run_pbg.py`, `submit_pbg.py` | The Python task and the job (`submit_pbg.py <name> photon`). `pbg.jar` and `run_pbg.py` go to the workspace folder `lpr` beside `lpr.jar`. |
| `analyse_pbg.py` | The tables, against `lambda-pruning-databricks/results/engine3-photon`. |
| `results/` | Raw JSON of both runs and `tables.md`. |

---

# DBR 19, Photon on and Photon off

The same probe, unchanged, on DBR 19. Runtime reported by the cluster: `DATABRICKS_RUNTIME_VERSION`
**19.14**, Spark **4.2.0**, Scala 2.13.18, Java 21.0.12 (cluster key `19.x-scala2.13`; Photon chosen by
`runtime_engine`). Same data, same 18 views, same single-node `r5d.large` dedicated cluster, rule never
installed (`extraOptimizations` size 0, recorded in `runtime.json`). Two clusters, one after the other,
user-approved; both jobs deleted, clusters terminated. The Pathling jars (built for Spark 4.0.2) loaded
and ran on 4.2.0 with no shim and no rebuild; `makeCopy` by field name and `classic.Dataset.ofRows` also
worked.

**Result: on DBR 19, compute-first prunes, with Photon on and off. The original plans still read
`name`/`address` whole, so DBR 19 does not prune plain views on its own.** That DBR 19 prunes after the
rewrite means it includes the nested-pruning patches that 17.3 lacked (the rewritten plan reads
`name[].family`; the same plan on 17.3 read all of `name`). It does not show which patch, and I have not
tested `filter` and `transform` apart.

- **Results equal in all 18 views, on all four runs** (Photon on/off, Parquet and Delta with deletion
  vectors): full rows, `except` both ways zero, counts, schema. Deletion vectors were added.
- **The rewrite applied every time** (54 per Parquet run, cumulative 108 after Delta, 0 no-ops, 0
  failures; each view has one `Generate`).
- **The `Project` survives under the `Generate` in every optimised plan**, Photon on and off.
- **Scans:** `PhotonParquetFileScanExec` with Photon on, `FileSourceScanExec` with Photon off.
- **Original plan, rule off:** whole `name`/`address` in all 14 name/address views, both modes (4.52 MB
  for `name` on Parquet with Photon). Unchanged from 17.3.
- **`prune_struct_fields`:** Photon off, none anywhere. Photon on, only `name.period.start`: 2
  occurrences in the original plan (as on 17.3) and 4 in the compute-first plan (Project and Filter,
  optimised and executed).
- **Plan stability:** reads and results identical on the repeat run in all views; the optimised-plan
  text differed between two runs in 5 views per run (as on 17.3), not investigated.

Classes against the rule-on reads of the **17.3** run (same data; the baseline is from 17.3, not
re-run on 19). 14 name/address views; the 4 singular-meta views read the same in every plan and are
excluded from the counts.

| | A (as tight as rule on) | B (partly) | C (whole) |
| --- | --- | --- | --- |
| Photon on | 9 | 3 | 2 |
| Photon off | 8 | 4 | 2 |

| View | Original | Compute-first (Photon on) | Compute-first (Photon off) | Rule on (17.3, Photon on) | Class on / off |
| --- | --- | --- | --- | --- | --- |
| `name.family` | name | name[].family | name[].family | name[].family | A / A |
| `name.given` | name | name[].given | name[].given | name[].given | A / A |
| `name.period.start` | name | name[].period | name[].period.start, period._start | name[].period (Photon gap) | A* / B |
| `name.where(period.start > @1990).family` | name | family, period.start, period._start | same | same | A / A |
| `name.where(use = 'nickname').given` | name | use, given | use, given | use, given | A / A |
| `name.exists()` | name | name | name | name[].use | C / C |
| `name.count()` | name | name | name | name[].use | C / C |
| `address.period.start` | address | address[].period | address[].period | address[].period.start | B / B |
| `address.city \| name.family` | name, address | name[].family, address[].city | same | same | A / A |
| forEach name / `family` | name | name[].family | name[].family | name[].family | A / A |
| forEach name / `period.start` | name | period.start, period._start | period.start, period._start | name[].period.start | B / B |
| `name.first().family` | name | name[].family | name[].family | name[].family | A / A |
| `name.where(family.exists()).given` | name | family, given | family, given | family, given | A / A |
| multi-column view with where and unionAll | id, name, birthDate, _birthDate, address | id, **name whole**, birthDate, address[].city | same | name[].use, family, address[].city, ... | B / B |

`A*`: with Photon on the rule's own 17.3 read for `name.period.start` is `name[].period`, the Photon
gap of the lambda-pruning check, so compute-first equals it; with Photon off the rule reads
`name[].period.start` and compute-first also reads `period._start` (class B).

- **Bytes** (Parquet, Photon on): `name.family` 4,521,838 original, 620,521 compute-first, 620,521 rule
  on (17.3); `name.given` 768,579 (equal); `address.city | name.family` 715,773 (equal). The multi-column
  view still reads 4,714,655 bytes against 819,807 for the rule, because its `where` keeps `name`
  whole. Photon-off bytes are of the same order (`name.family` 4,541,919 to 612,408, equal to the rule's
  612,408 from 17.3). All per-view numbers: `results/tables-dbr19-photon.md`, `results/tables-dbr19-nophoton.md`.
- **What stays blocked, as locally on 4.3.0:** `name.exists()` and `name.count()` (the presence-only
  `size(filter(name, ...))`, whole `name`), the `where` view, and the unused `period._start` (and, for
  `address.period.start`, the whole `period`) read for `period.start`. These are the cases where our rule
  still reads less.

## Conditions

- Parquet and Delta with deletion vectors only; column mapping, liquid clustering and UC managed tables
  not run. Photon off and on were each one run per table kind.
- Rule-on numbers are the 17.3 ones, not measured on 19; on 19 the rule itself was not run, so whether
  the rule needs the `makeCopy` shim there is unknown.
- The rewrite is a probe plan rewrite, not the engine change.
- Raw JSON, `runtime.json` and tables: `scripts/annotations-design/project-before-generate-databricks/results/pbg19-photon`,
  `pbg19-nophoton`. `submit_pbg.py` takes the runtime key as its third argument.
