# What does Spark 4.3.0 prune on its own, without the lambda-pruning rule?

A re-measurement of the cases in
[annotations-design-lambda-pruning-rule.md](annotations-design-lambda-pruning-rule.md) on Apache
Spark 4.3.0, with Pathling's rule off, to see what upstream now covers and what is left to the rule.

**Result: Spark 4.3.0 closes part of the gap, and the part is the simple one.** Of the 24 SQL cases
(1a to 4d), 2 are controls that read whole by design (1e, 1f). Of the other 22:

- **A, upstream prunes as well as the rule (7):** 1a, 1b, 1c, 1j, 2a, 2b, 4d. All are
  `filter`/`transform` chains over a column, with no other expression in the way. (1j and 4d are
  controls for the rule's guard and for plain field access; 4d already pruned on 4.0.2.)
- **B, upstream prunes partly (2):** 1k and 3a. Upstream gets down to the right leaf columns of the
  parent but reads the whole of an annotation group where the rule reads one leaf of it.
- **C, upstream still reads whole (13):** 1d, 1g, 1h, 1i, 2c, 3b, 3c, 3d, 3e, 3f, 4a, 4b, 4c.
  Every one has `inline` (a `Generate`), `exists`, `zip_with`, or a `size` over a `filter` as the
  blocker.
- **The real engine views are all C.** Every one of the 18 views of case 5 reads exactly what it read
  on 4.0.2 without the rule (the whole of `name` or `address`), although the same shapes written by
  hand in SQL (1a to 1c) now prune. See [the engine views](#case-5-engine-views).
- The rule still compiles and runs unchanged on 4.3.0, and its results equal the rule-off results in
  all 24 SQL cases and all 18 engine views. Its read schemas are the same as on 4.0.2 in every case
  except 1j, where it no longer needs its guard.

What is left to the rule on 4.3.0 is `Generate`/`inline`, `exists`, `zip_with`, `arrays_zip`, the
`size` of a `filter`, and demand inside an annotation group. Those are the expressions the engine's
views produce.

## Results

Reads are the leaves Parquet reads (the `ReadSchema` of `FileSourceScanExec`), in the compact form
of `CaseProbe`, on the 203-row `cases.parquet` of the original spike. The 4.0.2 columns are copied
from the original write-up and `results.txt`.

| Case | 4.0.2 rule off | 4.0.2 rule on | 4.3.0 rule off | Class | Blocker |
| --- | --- | --- | --- | --- | --- |
| 1a `filter(name, x -> x.family is not null).family` | `name` | `name[].family` | `name[].family` | A | |
| 1b engine traversal, two nested `filter`s | `name` | `name[].family` | `name[].family` | A | |
| 1c `transform(name, x -> x.family)` | `name` | `name[].family` | `name[].family` | A | |
| 1d 1b inside `inline(array(struct(...)))` | `name` | `name[].family` | `name` | C | `inline` |
| 1e control: whole output | `name` | `name` | `name` | control | |
| 1f control: `to_json(x)` | `name` | `name` | `name` | control | |
| 1g `exists(filter(name, ...), x -> x.period.start > c)` | `id`, `name` | `id`, `name[].period.start` | `id`, `name` | C | `exists` |
| 1h `size(filter(name, x -> x is not null))` | `name` | `name[].id` | `name` | C | presence only: `size` of a `filter` |
| 1i `size(filter(location, ...))` | `location` | `location[].extension[].url` | `location` | C | same as 1h |
| 1j guard: `_x` arrays pruned differently meet in `concat` | `_birthDate`, `name` | same (the rule bailed out) | `_birthDate`, `name[]._family` | A | |
| 1k `filter(name, x -> x._family is not null).family` | `name` | `name[].family`, `name[]._family.id` | `name[].family`, `name[]._family` | B | a null test on a struct reads all of it |
| 2a `location.period.where(start > c).start` | `location` | `location[].period.start` | `location[].period.start` | A | |
| 2b 2a comparing `p._start.__start` | `location` | `location[].period.start`, `location[].period._start.__start` | same as rule on | A | |
| 2c 2a inside `inline` | `location` | `location[].period.start` | `location` | C | `inline` |
| 3a `arrays_zip(flatten(...timing.event), flatten(...timing._event).__start)` | `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].__start` | `dosage[].timing.event`, `dosage[].timing._event` | B | `arrays_zip` (the leaf of `_event` is not pruned) |
| 3b 3a inside `inline` | `dosage` | as 3a rule on | `dosage` | C | `inline` |
| 3c `zip_with` pairing, output `.value`, in `inline` | `dosage` | `dosage[].timing.event`, `dosage[].timing._event[].id` | `dosage` | C | `inline`, `zip_with` |
| 3d 3c filtering on `e.meta.__start` | `dosage` | `...event`, `..._event[].__start` | `dosage` | C | `inline`, `zip_with` |
| 3e `arrays_zip` pairing, output `.event`, in `inline` | `dosage` | `...event`, `..._event[].id` | `dosage` | C | `inline`, `arrays_zip` |
| 3f 3e filtering on `e._event.__start` | `dosage` | `...event`, `..._event[].__start` | `dosage` | C | `inline`, `arrays_zip` |
| 4a singular `struct(value, meta)`, output only, in `inline` | `birthDate`, `_birthDate` | `birthDate` | `birthDate`, `_birthDate` | C | `inline` |
| 4b 4a comparing `e.meta.__start` | `birthDate`, `_birthDate` | `birthDate`, `_birthDate.__start` | `birthDate`, `_birthDate` | C | `inline` |
| 4c 4a with `v.meta is not null` | `birthDate`, `_birthDate` | `birthDate`, `_birthDate.id` | `birthDate`, `_birthDate` | C | `inline` |
| 4d plain `select s.value` | `birthDate` | `birthDate` | `birthDate` | A | |

"Whole" is dropped from the cells: every leaf named is read in full, and a bare column name is read
entirely. The "same as rule on" cells are identical to the 4.0.2 rule-on cell.

How each blocker was attributed. `inline` is isolated by the pairs 1b/1d and 2a/2c: the same
expression prunes on 4.3.0 without the `inline` wrapper and reads whole with it. `exists` rests on
1g alone, and agrees with the source of `SchemaPruning.scala` at v4.3.0, which has no
`ArrayExists`/`ArrayForAll` case (SPARK-57175 is on master only). `arrays_zip` is isolated by 3a,
which has no `inline` and still reads `_event` whole. `zip_with` and the singular `inline` cases
(4a to 4c) were not run without `inline`, so for them `inline` is the blocker shown by analogy with
the pairs above and the `Generate` gap in the source (SPARK-34956), not by a separate run. 1h and 1i
were not isolated further: `size` over a `filter` is a use of the whole element, and upstream does
not narrow it to a leaf.

### Case 5, engine views

`EngineProbe` ran on the 4.3.0 classpath. All 14 views over `name` and `address`, and the
multi-column view with `where` and `unionAll`, read the whole of the column with the rule off, the
same as on 4.0.2, so none of them improved. The rule reads the same leaves as on 4.0.2
(`name[].family`, `name[].period.start`, and so on), and results are equal.

The four singular-meta views read the same with and without the rule on both versions, because the
stored `_birthDate` there holds only `__start` and `__end`; they say nothing about upstream.

The cause was not diagnosed. The plans the engine builds for `name.family` differ from 1a to 1c,
which prune. The likely reason is the `Generate` and the other expressions the view adds around the
traversal, as in 1d, but this was not checked against the plan.

### The rule on 4.3.0

`LambdaPruning.scala` compiled without change against 4.3.0 and ran in every case. Rule on equals
rule off in rows and output schema in all 24 SQL cases and 18 engine views. It did not bail out in
1j, as it did on 4.0.2, because the two arrays reaching `concat` are now pruned the same way by
upstream. The edge-row check (`e1` to `e3`: null elements, an element present only through its
extension, a non-null `_birthDate` with null leaves) printed the same table as on 4.0.2.

## Method

- A scratch `pom.xml` with `spark-sql_2.13`, `spark-catalyst_2.13` and `spark-core_2.13` at 4.3.0,
  and `mvn dependency:build-classpath`, gave 176 jars. Spark 4.3.0 uses Scala 2.13.18, Parquet 1.17.0
  and Hadoop 3.5.0. The probe was compiled with the 2.13.16 compiler from `~/.m2` against the
  2.13.18 library, which gave no error.
- `CaseProbe.scala` and `LambdaPruning.scala` are unchanged copies of the originals. `CaseProbe`
  writes the 203-row Parquet file, runs each query with the rule uninstalled and installed, and
  prints the read schema and whether results are equal. Local mode, two cores, vectorised reader,
  adaptive execution off.
- `EngineProbe.java` and `gen_patients.py` are unchanged copies. Its classpath is the 4.3.0 jars, the
  third-party classpath of the 4.0.2 runs with the `org/apache/spark/` jars removed, and the Pathling
  10.0.0-SNAPSHOT jars built against 4.0.2. It ran on the first attempt, with no rebuild.
- The scripts are in `scripts/annotations-design/spark-4-3-upstream/`: `build.sh`, `run.sh` for
  cases 1 to 4 and `engine.sh` for case 5. Classpath, data and logs are in
  `.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-upstream/`
  (`cases-v.log`, `engine.log`).

## Versions

- Spark 4.3.0 (Maven Central), Java 21.0.3, Scala 2.13.18 at runtime.
- The 4.0.2 columns are from the earlier spike: Spark 4.0.2, Scala 2.13.16.
- Source check: `SchemaPruning.scala` at the v4.3.0 tag handles `ArrayTransform` (SPARK-57022),
  `ArrayFilter` and `ArraySort` (SPARK-57176), and not `ArrayExists`/`ArrayForAll`, `ZipWith`,
  `ArraysZip`, `ArrayAggregate` or `Generate`. The measurements agree with that source in every case.

## Delta

Not run. `delta-spark_2.13` on Maven Central goes up to 4.4.1. Its 4.3.0 POM depends on Spark 4.1.0
and its 4.4.1 POM on Spark 4.2.0, so no release targets Spark 4.3. The 4.0.0 release the earlier
spike used is built for Spark 4.0 and was not forced onto 4.3.

## Caveats

- Results across versions are compared by read schema and by rule-on equals rule-off within 4.3.0.
  Row output on 4.0.2 and 4.3.0 was not compared against each other; the probe does not write it
  out. The 203-row counts agree.
- Bytes read were not measured; the probe prints read schemas only, and the file is small.
- The engine run mixes 4.3.0 Spark jars with Pathling jars built for 4.0.2 and a Delta 4.0.0 jar.
  It ran without a binary-compatibility error, but this is not a supported combination, and the
  engine view results show only what the plans read, not that the engine is supported on 4.3.
- Case 5 uses 22 Patients; the case table uses one synthetic file with five 40-character extension
  payloads per group. They show which leaves are read, not the size of a saving.
- Only the vectorised reader was run, as in the original; `-rowreader` and `-aqe` were not.
- SPARK-57175 (`exists`) is on master. A release that carries it would change 1g and, with it,
  views that filter on `exists`; the other blockers would stay.
