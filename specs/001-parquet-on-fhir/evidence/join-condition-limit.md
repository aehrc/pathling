# M2 review: an engine-built column in a join condition

**Result: a regression from M2, on both layouts, accepted by the owner as a
known issue on 2026-09-29, to be revisited.** A column built by
`fhirPathToColumn` or `searchToColumn` that reads a column, used directly in a
join condition, fails with `INTERNAL_ERROR` where it used to work. Joining first
and then filtering still works. SQL-on-FHIR view joins on `getResourceKey()` and
`getReferenceKey()` are not affected. The cause is an assertion in Spark's
analyzer that the tolerant table-column reference reaches in a plan with two
children. Two fixes were probed and work; neither is implemented. Decision 75's
join-condition limit records the decision, and its "When to revisit" list the
way forward.

## Conditions

- Spark 4.0.2 (Scala 2.13), `local[2]`, with ANSI on.
- HEAD means `b2332fe7aa`, the tip of `issue/2762` when probed. The base is
  `18e59220ec`, before M2.
- The data is `server/src/test/resources/import-data/ndjson`: 3697 encounters,
  with their patients and observations. Each resource type is written to
  Parquet and read back, in the previous layout through `PathlingContext` and in
  the new layout through `FhirJsonReader`, then registered in a
  `DatasetSource`.
- The probes are single-file Java programs run against built jars, with no
  change to the tree except the patch below:
    - `scripts/join-condition/JoinProbe.java` runs at HEAD, on both layouts;
    - `scripts/join-condition/BaseJoinProbe.java` runs at the base, on the
      previous layout only, because the engine there reads no other;
    - `scripts/join-condition/probe-patch.diff` is a throwaway change to
      `UnresolvedColumnOrNull.mapChildren` in `encoders`, which selects one of
      three candidate fixes by `-Dpathling.probe.variant`.

## The reproduction

```java
Dataset<Row> patients = ds.read("Patient");
Dataset<Row> encounters = ds.read("Encounter").select("subject", "status");
Column key = encounters.col("subject.reference").endsWith(patients.col("id"));
Column male = pc.fhirPathToColumn("Patient", "gender = 'male'");

encounters.join(patients, male.and(key)).count();     // base 2100, HEAD INTERNAL_ERROR
encounters.join(patients, key).filter(male).count();  // 2100 on both
```

## Results

Base is the previous layout only. Where the two layouts agree, one value is
given; otherwise it is previous / new. A, B and C are the patched variants,
described under the options below.

| Case                                                         | Base                | HEAD               | A                                 | B                   | C                   |
| ------------------------------------------------------------ | ------------------- | ------------------ | --------------------------------- | ------------------- | ------------------- |
| Join condition, `gender = 'male'`                            | 2100                | **INTERNAL_ERROR** | 2100                              | 2100                | 2100                |
| Join condition, `name.family.exists()`                       | 3697                | **INTERNAL_ERROR** | 3697                              | 3697                | 3697                |
| Join condition, `birthDate > @1950-01-01`                    | 3082                | **INTERNAL_ERROR** | 3082                              | 3082                | 3082                |
| Join condition, `extension('…us-core-race').exists()`        | 0                   | **INTERNAL_ERROR** | UNRESOLVED_COLUMN `extension` / 0 | 0                   | 0                   |
| Join condition, `deceasedBoolean.empty()`                    | 3697                | **INTERNAL_ERROR** | 3697 / UNRESOLVED_COLUMN          | 3697                | 3697                |
| Join condition, `photo.empty()`                              | 3697                | **INTERNAL_ERROR** | 3697 / UNRESOLVED_COLUMN          | 3697                | 3697                |
| Join condition, `true` (reads no column)                     | 3697                | 3697               | 3697                              | 3697                | 3697                |
| Join condition, Observation `valueQuantity.value > 50`       | 2116                | **INTERNAL_ERROR** | 2116                              | 2116                | 2116                |
| Join, then filter, each expression above                     | as the join         | same as base       | same as base                      | same as base        | same as base        |
| Join condition, `id.exists()`, `id` on both sides            | AMBIGUOUS_REFERENCE | **INTERNAL_ERROR** | AMBIGUOUS_REFERENCE               | AMBIGUOUS_REFERENCE | AMBIGUOUS_REFERENCE |
| Join condition, `not(text.exists())`, `text` on neither side | UNRESOLVED_COLUMN   | **INTERNAL_ERROR** | UNRESOLVED_COLUMN                 | 3697                | 3697                |
| Self-join condition, `gender = 'male'`                       | AMBIGUOUS_REFERENCE | **INTERNAL_ERROR** | AMBIGUOUS_REFERENCE               | AMBIGUOUS_REFERENCE | AMBIGUOUS_REFERENCE |
| Self-join, then filter, `gender = 'male'`                    | AMBIGUOUS_REFERENCE | 0, silently        | 0, silently                       | 0, silently         | AMBIGUOUS_REFERENCE |
| `select("id")`, then filter, `gender = 'male'`               | 60                  | 0                  | 0                                 | 0                   | 0                   |
| Raw Spark, `columnOrNull("x")` in a join condition, present  | n/a                 | **INTERNAL_ERROR** | 1                                 | 1                   | 1                   |
| Raw Spark, `columnOrNull("z")` in a join condition, absent   | n/a                 | **INTERNAL_ERROR** | UNRESOLVED_COLUMN                 | 2                   | 2                   |
| Raw Spark, `columnOrNull("x")` in a `left_semi` join         | n/a                 | **INTERNAL_ERROR** | 1                                 | 1                   | 1                   |
| Raw Spark, `resolveOrNull` of a struct field in a join       | n/a                 | 1                  | 1                                 | 1                   | 1                   |

- **Every expression that reads a column fails** at HEAD in a join condition,
  on both layouts: a primitive, a nested element, a date, a quantity, an
  extension and an absent element alike. An expression that reads no column
  works.
- **A struct field is unaffected.** `ResolveOrNull` resolves against its
  resolved parent, not the plan's children. Only the root reference fails.
- **The `select("id")` row** is decision 75's separate dropped-column limit. It
  is shown because no variant changes it.
- **The "Self-join, then filter" row has since changed.** The tolerant reference
  now falls back to a typed null only when no column matches, so HEAD raises
  `AMBIGUOUS_REFERENCE` there, as base does. The table records the measurement
  as it was taken.

The raw result files are not kept. `JoinProbe` prints one line per case, so a
rerun reproduces them.

## The cause

Every root element now goes through the tolerant table-column reference,
`UnresolvedColumnOrNull`. Its child is a `GetViewColumnByNameAndOrdinal`, and
its `mapChildren` catches the `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` that node raises
when the name is missing, answering a typed null.

For a `Join`, `ResolveReferences` resolves the condition through
`ColumnResolutionHelper.resolveExpressionByPlanChildren`. In Spark 4.0.2 that
supplies two callbacks:

```scala
// ColumnResolutionHelper.scala:520-526
resolveColumnByName = nameParts => {
  q.resolveChildren(nameParts, conf.resolver)
},
getAttrCandidates = () => {
  assert(q.children.length == 1)
  q.children.head.output
},
```

A `GetViewColumnByNameAndOrdinal` is resolved through the second
(`ColumnResolutionHelper.scala:160-162`). A `Join` has two children, so the
assertion fails. The `AssertionError` is not the exception the catch looks
for, and Spark reports it as `INTERNAL_ERROR`, whether or not the column
exists. The trace, abridged:

```text
Caused by: java.lang.AssertionError: assertion failed
  at scala.Predef$.assert(Predef.scala:264)
  at ...ColumnResolutionHelper.$anonfun$resolveExpressionByPlanChildren$2(ColumnResolutionHelper.scala:524)
  at ...ColumnResolutionHelper.$anonfun$resolveExpression$1(ColumnResolutionHelper.scala:162)
  ...
  at au.csiro.pathling.encoders.UnresolvedColumnOrNull.mapChildren(Expressions.scala:448)
  ...
  at ...ColumnResolutionHelper.resolveExpressionByPlanChildren(ColumnResolutionHelper.scala:528)
  at ...Analyzer$ResolveReferences.resolveExpressionByPlanChildren(Analyzer.scala:1453)
  at ...Analyzer$ResolveReferences$$anonfun$doApply$3.applyOrElse(Analyzer.scala:1779)
```

The base emitted a plain `UnresolvedAttribute`, which is resolved through the
first callback, `resolveChildren`, across both sides. A filter over the joined
result has one child, so joining first and then filtering works.

## What this corrects

Decisions 73 and 75 said the base "failed with `AMBIGUOUS_REFERENCE`". That is
true only where both sides have a column of that name, as in a self-join. Where
exactly one side has it, the base worked. Where neither has it, it failed with
`UNRESOLVED_COLUMN`. So M2 is a regression, not a change of error.

## What is not affected

SQL-on-FHIR view joins on `getResourceKey()` and `getReferenceKey()`. Their
FHIRPath columns are compiled inside each view's single-child projection, and
the join compares plain output columns.
`fhirpath/src/test/java/au/csiro/pathling/views/ReferenceKeyJoinTest.java`
covers this and passes on both layouts. The T038a spike found no main code in
`fhirpath`, `library-api` or `server` that puts an engine-built column into a
join condition. The pattern that fails, a caller placing one there directly, is
rare.

## The options

The way forward follows decision 75's "When to revisit" list. Its first item,
fixing the join and ambiguity failures, is option C, with B as its variant.

| Option                     | What it does                                                                                                                                                 | Probed                             | Consequence                                                                                                                                                                                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **C** (primary)            | Try a plain `UnresolvedAttribute` first. If it stays unresolved, fall back to the view-column path, and treat the assertion as absence.                      | Variant `c`. Works on both layouts | Also closes the self-join limit: an ambiguous name fails with `AMBIGUOUS_REFERENCE` instead of a silent null. A plain name could resolve to something else first: a lambda variable, an outer reference, or a literal function such as `current_date` (`ColumnResolutionHelper.scala:172`) |
| **B** (C's variant)        | Catch the assertion, retry with a plain attribute, and answer a typed null if that stays unresolved.                                                         | Variant `b`. Works on both layouts | Leaves the self-join limit as it is                                                                                                                                                                                                                                                        |
| Fallback: an analyzer rule | A custom analyzer rule, the list's second item.                                                                                                              | No                                 | Needs `spark.sql.extensions`, which takes effect only when a session is created. A user with an existing session, on Databricks, or in Python and R has to configure it before Pathling is involved, and some cannot                                                                       |
| Fallback: the schema       | Drop the schema-agnostic requirement for the FHIRPath evaluator, and resolve an unknown element statically from the dataset's schema when a column is built. | No                                 | Supersedes decision 75's principle, and needs a schema at every place that compiles a column, including `fhirPathToColumn` and `searchToColumn`, which have none today                                                                                                                     |
| **G** (interim)            | Replace `INTERNAL_ERROR` with a clear `AnalysisException` that says to join first, then filter.                                                              | No                                 | A mitigation until a fix lands, not a fix                                                                                                                                                                                                                                                  |
| A (probe control)          | Catch the assertion and retry with a plain attribute, with no null.                                                                                          | Variant `a`                        | Not tolerant: an absent element fails with `UNRESOLVED_COLUMN`. It is why B needs the typed null                                                                                                                                                                                           |
| Rejected: a null           | Answer the fallback null on the assertion.                                                                                                                   | No                                 | Silently wrong where the column exists                                                                                                                                                                                                                                                     |

C and B both rest on Spark raising that assertion for a multi-child plan. That
is an analyzer internal, so either needs a plan-shape test to pin it, and a
Spark upgrade that changes it would show there. The two fallbacks apply only if
C and B prove unworkable.

## Tests

- **At the base, `18e59220ec`, no test uses an engine-built column in a join
  condition**, which is why the regression was not caught.
- **At HEAD, `ResolveOrNullTest.knownLimitJoinConditionFailsWithInternalError`**
  in `encoders` pins the limit. It stays as it is until the fix. Its `other`
  fixture re-reads Parquet, so it needs rewriting when the fix lands.
- **With the patch applied**, `ResolveOrNullTest` and
  `TransformTreeTerminationTest` in `encoders`, 54 tests, were run under B and
  under C. Only the pins failed:
    - under both, the join pin, with an `ExtendedAnalysisException` where it
      expects `INTERNAL_ERROR`;
    - under C, also `knownLimitAmbiguousNameGivesSilentNull`, which now raises
      `AMBIGUOUS_REFERENCE`, as that limit being closed predicts.

## Rerunning it

```bash
# At HEAD: the Pathling jars, and the third-party classpath.
mvn package -pl library-api -am -DskipTests -Djacoco.skip=true
mvn dependency:build-classpath -pl library-api -Dmdep.outputFile=/tmp/cp.txt

# The base, in a separate tree, previous layout only.
git worktree add /tmp/base 18e59220ec
(cd /tmp/base && mvn package -pl library-api -am -DskipTests -Djacoco.skip=true)

S=specs/001-parquet-on-fhir/evidence/scripts/join-condition
DATA=server/src/test/resources/import-data/ndjson

$S/run-base.sh /tmp/base /tmp/cp.txt $DATA /tmp/join-scratch   # base
$S/run.sh . /tmp/cp.txt "" $DATA /tmp/join-scratch             # HEAD, unpatched
$S/run.sh . /tmp/cp.txt "" $DATA /tmp/join-scratch trace       # with the first stack trace

# The variants: compile the patched encoders, keep the classes, then revert.
git apply $S/probe-patch.diff
mvn compile -pl encoders -am -Djacoco.skip=true -Dspotless.check.skip=true
cp -R encoders/target/classes /tmp/patched-encoders-classes
git apply -R $S/probe-patch.diff
for v in a b c; do
  JAVA_OPTS=-Dpathling.probe.variant=$v \
    $S/run.sh . /tmp/cp.txt /tmp/patched-encoders-classes $DATA /tmp/join-scratch
done
```

The patched classes go ahead of the jars on the classpath, so they replace the
unpatched `UnresolvedColumnOrNull`. The unit runs under B and C use the same
patch, with Spotless skipped and the variant property passed to the forked test
JVM.
