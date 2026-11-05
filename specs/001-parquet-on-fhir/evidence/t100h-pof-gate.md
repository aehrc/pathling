# The gate before the `fhirpath` suites switch

T100h is the gate for Phase 9a. Before the layout dimension's default flips to
the new layout (T100g), it runs the full FHIRPath suite, both YAML conformance
baselines and the SQL-on-FHIR compliance suite with `-Dpathling.testLayout=pof`,
and sorts every failure into one of five causes: shape reconciliation, decimal
scale, output type (T111), an obsolete exclusion, or an engine defect. The count
of reconciliation failures decides which of Phase 9 must land in M2 and which
may move to M5. Every change to an existing test or exclusion that the result
calls for is listed for the programme owner's approval, per test (decision 73).

**Result: 13 failing cases, none from reconciliation, decimal scale or T111.**

| Cause                                                                 |  Cases |
| --------------------------------------------------------------------- | -----: |
| Shape reconciliation                                                  |      0 |
| Decimal scale                                                         |      0 |
| Output type (T111)                                                    |      0 |
| Obsolete exclusion, layout-dependent                                  |      3 |
| Engine defect: test-harness fragility, exposed by the plan shape      |      6 |
| Engine defect, independent of the layout, exposed by the plan shape   |      2 |
| Engine defect, genuinely layout-dependent: `instant` was never ported |      2 |
| **Total**                                                             | **13** |

- **Nothing in Phase 9 moves to M5.** No existing test reached reconciliation on
  the new layout, and all of Phase 9 has already landed: T104a, T104b, T109,
  T113a and T113b are done. T104b was the candidate for deferral, and it is
  already complete in M2.
- **No failure is a new-layout bug in the ported features.** 8 of the 13 come
  from the fixture, not from the layout. The new-layout arm reads JSON, which
  runs a real Spark job. The previous-layout arm builds a local relation, which
  the optimiser evaluates on the driver. Both effects reproduce on the
  previous layout once its data is read from Parquet.
- **The SQL-on-FHIR compliance suite is green on both layouts**, with 124 tests
  each.
- **No fix was made.** None of the failures is a clear new-layout bug in code
  whose previous-layout answer is settled. So each resolution below is a
  proposal, and 5 of them need the owner's approval first.
- **The owner approved every item on 2026-09-28.** The list and what was done are
  under [Approvals needed](#approvals-needed).

## Conditions

|         |                                                                                          |
| ------- | ---------------------------------------------------------------------------------------- |
| Commit  | `d3b7cb4ff1` (`issue/2762`)                                                              |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 26.6.2                                              |
| JVM     | Java HotSpot 21.0.3+7-LTS-152                                                            |
| Spark   | 4.0.2 (Scala 2.13)                                                                       |
| Session | The `fhirpath` test session (`@SpringBootUnitTest`), ANSI mode on, `-Duser.timezone=UTC` |

The runs, each a reactor build so that `fhirpath` resolved `encoders` and
`terminology` from the reactor rather than `~/.m2`:

```bash
# The FHIRPath suite and both YAML baselines.
mvn test -pl encoders,terminology,fhirpath -am -Dpathling.testLayout=pof \
  -Dmaven.test.failure.ignore=true

# The SQL-on-FHIR compliance suite, once per layout.
mvn test -pl encoders,terminology,fhirpath -am -Dpathling.testLayout=pof \
  -Dtest=FhirViewShareableComplianceTest -Dsurefire.failIfNoSpecifiedTests=false
mvn test -pl encoders,terminology,fhirpath -am \
  -Dtest=FhirViewShareableComplianceTest -Dsurefire.failIfNoSpecifiedTests=false
```

The compliance suite is not in the first run. `fhirpath/pom.xml` excludes
`FhirViewShareableComplianceTest` from `default-test`, and `-PsofComplianceReport`
runs it with its exclusions disabled and failures ignored. So it was run on its
own, with `-Dtest`, which overrides the exclude, and without the profile, so
that its `row_index` exclusions stay in force. `FhirViewTest.getDataSource`
follows the layout dimension. The `sql-on-fhir` submodule was already
initialised.

The probes are in `scripts/t100h/`, and their output is in `t100h-pof-gate.txt`
beside this file. Each is a plain Java program, run on the `fhirpath` test
classpath. To rerun them:

1. Build the dependency classpath with
   `mvn -o dependency:build-classpath -f fhirpath/pom.xml -Dmdep.includeScope=test -Dmdep.outputFile=cp.txt`.
2. Remove the Pathling jars from `cp.txt`, because the reactor run was `test`,
   not `install`, so the jars in `~/.m2` are stale.
3. Put the worktree's `target/classes` and `target/test-classes` for
   `fhirpath`, `terminology`, `encoders`, `utilities`, `fhir-schema` and `io`
   ahead of what remains.
4. Compile with `javac -proc:none`.
5. Run with the surefire JVM options from the root `pom.xml`: `-Duser.timezone=UTC`,
   `-Djava.security.manager=allow`,
   `--add-exports=java.base/sun.nio.ch=ALL-UNNAMED`,
   `--add-opens=java.base/java.net=ALL-UNNAMED` and
   `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED`.

Two probes need more than that:

- `TraceProbe` also takes `-Dspark.serializer.extraDebugInfo=false`, and a
  directory for its Parquet output.
- `InstantProbe` registers the Pathling UDFs itself, because it has no Spring
  session.

## Counts

| Run                                         | Tests | Failures | Errors | Skipped |
| ------------------------------------------- | ----: | -------: | -----: | ------: |
| `encoders`, `pof`                           |   598 |        0 |      0 |       0 |
| `terminology`, `pof`                        |   535 |        0 |      0 |       0 |
| `fhirpath`, `pof`                           |  7272 |        7 |      6 |     921 |
| of which `YamlReferenceImplTest`            |  1821 |        3 |      0 |     917 |
| of which `YamlFhirPathTest`                 |  1268 |        0 |      0 |       1 |
| `FhirViewShareableComplianceTest`, `pof`    |   124 |        0 |      0 |       0 |
| `FhirViewShareableComplianceTest`, previous |   124 |        0 |      0 |       0 |
| `fhirpath`, default, at the same commit     |  7280 |        0 |      0 |     924 |

- The `pof` run is the same 13 failing cases as every opt-in run since port
  step 5.
- The per-class counts of the two `fhirpath` runs are identical, except for
  `YamlReferenceImplTest`. It skips 920 cases on the default run and 917 under
  `pof`, because the three cases below run there rather than being skipped as
  excluded. The per-class `Tests run` lines in the `fhirpath` section of each
  log sum to 7296 in both runs. Yet the summaries say 7280 and 7272. So the gap
  of 8, which has been constant since port step 5, comes from how Surefire
  aggregates the summary over the failing nested classes, not from which tests
  run.

## What the zeros cover

The layout dimension governs four fixture entry points: `ObjectDataSource`,
`HapiResolverFactory`, `FhirResolverFactory` (the YAML runners) and
`FhirViewTest.getDataSource`. These tests encode with `FhirEncoders` directly,
and so stay on the previous layout under `pof`:

- `AbsentElementTest`
- `ProjectedColumnTypeTest`
- `SiblingCombinationTest`
- `DivergentSchemaViewTest`
- `YamlTestRunnerTest.testJsonModel`

So the zero for T111 does not cover `ProjectedColumnTypeTest`, which is T111's
own test. It covers the view tests that go through `FhirViewTest` and
`ObjectDataSource`, and the SQL-on-FHIR compliance suite. The tests that were
written for the new layout, such as `ShapeReconciliationTest` and
`DecimalCollectionTest`, build both layouts themselves and pass on both.

## Every failure

| #   | Test                                                                                                                                                                                                                                                                                         | Cause                                     | Root cause                                                                                                                                                                                                                                                                                    | Resolution                                                                                        | Kind                     | Milestone |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------ | --------- |
| 1–6 | `TraceFunctionTest$CollectorTests`: `collector_capturesCorrectFhirType_complex`, `…_primitive`, `collector_withProjection_capturesProjectedFhirType`, `collector_chainedTrace_innerNotDoubleEvaluated`, `collector_capturesEntriesWithLabel`, `collector_twoTraceCallsProduceDistinctLabels` | Engine defect: test-harness fragility     | The test hands a raw `ListTraceCollector`, which is not `Serializable`, to `DatasetEvaluatorBuilder`. A real job serialises the task closure and fails with `Task not serializable`. `SerializationDebugger` cannot initialise on Java 21 without an `--add-opens`, so it hides that failure. | Wrap the collector in `TraceCollectorProxy`, as `SingleInstanceEvaluator` does.                   | (c)                      | M2        |
| 7–8 | `TraceFunctionTest$TraceEntryCountTest.entryCount[18]` (`'a'.trace('t') = 1`) and `[19]` (`'a' = 1.trace('t')`)                                                                                                                                                                              | Engine defect, independent of the layout  | `TraceExpression.nullable` is its child's, so over a literal it is non-nullable. The optimiser then folds `isnull(trace('a'))` to `false`, collapses the comparison to `false` and drops the trace. A local relation is evaluated before that rule runs.                                      | `TraceExpression` and `TraceProjectionExpression` report `nullable = true`.                       | (a)                      | M2        |
| 9   | `AnsiTypeHintingTest.defaultMiscMappings[6]` (`issued`, no declared type)                                                                                                                                                                                                                    | Engine defect, genuinely layout-dependent | The previous layout stores `instant` as `TimestampType`, and the new layout stores it as text. No port step covered temporal types, so the column's default type follows the storage type: `TimestampType` becomes `StringType`.                                                              | Owner decides the query-time type of `instant`: option A or option B below.                       | (a) under A; (c) under B | M2        |
| 10  | `AnsiTypeHintingTest.miscAnsiCasts[2]` (`issued` as `TIMESTAMP WITHOUT TIME ZONE`)                                                                                                                                                                                                           | Engine defect, genuinely layout-dependent | Casting `2023-01-01T12:00:00+10:00` to `TIMESTAMP_NTZ` discards the offset and gives the wall time, 12:00. The previous layout casts an instant, which gives 02:00 in UTC. For an instant, 12:00 is wrong.                                                                                    | Fixed by either option below. No change to the test.                                              | (a)                      | M2        |
| 11  | `YamlReferenceImplTest.testTypes[28]` (`StructureDefinition.snapshot.element.type.code is uri`)                                                                                                                                                                                              | Obsolete exclusion, layout-dependent      | The encoder refuses `StructureDefinition` (`ResourceTypes.UNSUPPORTED_RESOURCES`), but the new-layout transform reads it. So the case passes, and the `^StructureDefinition` rule (#2418), expecting an error, reports it.                                                                    | Make the #2418 rule apply to the previous layout only.                                            | (b)                      | M2        |
| 12  | `YamlReferenceImplTest.testTypes[27]` (`StructureDefinition.snapshot.element.type is Element`)                                                                                                                                                                                               | Obsolete exclusion, layout-dependent      | Same as 11. Once the resource is readable, the case gives `false` rather than `true`. The cause is the missing type-hierarchy support that #2524 tracks, and which already excludes `Patient.contact is Element`.                                                                             | Make the #2418 rule apply to the previous layout only, and add this expression to the #2524 rule. | (b)                      | M2        |
| 13  | `YamlReferenceImplTest.testFhirR4[218]` (`ValueSet.expansion.repeat(contains).count() = 10`)                                                                                                                                                                                                 | Obsolete exclusion, layout-dependent      | The rule "Recursive ValueSet.expansion.contains not encoded" exists because the encoder skips the recursive `contains` field. The new layout keeps it, so the case passes.                                                                                                                    | Make that rule apply to the previous layout only.                                                 | (b)                      | M2        |

All thirteen belong in M2, because each one fails the build the moment T100g
flips the default.

## 1–6. The trace collector

`CollectorTests.setUpCollector` passes a `ListTraceCollector` straight to
`DatasetEvaluatorBuilder.withTraceCollector`. `TraceExpression` holds the
collector as a field, so the collector is part of every task closure. The
production path, `SingleInstanceEvaluator`, wraps it first in
`TraceCollectorProxy`. That proxy is `Serializable`, and it resolves the real
collector through a JVM-wide registry.

The test depends on the layout only through the plan:

- **Previous layout.** `LayoutDatasets.fromResources` returns a `LocalRelation`.
  The optimiser's `ConvertToLocalRelation` evaluates the projection on the
  driver, so no closure is ever serialised.
- **New layout.** The data is read with `spark.read().json(...)`, which gives a
  `LogicalRDD`. So `collect` runs a job, and the closure is serialised.

The probe (`TraceProbe`) evaluates `Patient.name.trace('names')` three ways:
over the local relation, over the same previous-layout data written to Parquet
and read back, and over the new layout. It also sets
`spark.serializer.extraDebugInfo=false`, so that the real exception surfaces:

| Data                               | Raw collector                                                           | Proxied collector            |
| ---------------------------------- | ----------------------------------------------------------------------- | ---------------------------- |
| Previous layout, local relation    | passes, 1 entry, `HumanName`                                            | passes, 1 entry, `HumanName` |
| Previous layout, read from Parquet | `Task not serializable`: `NotSerializableException: ListTraceCollector` | passes, 1 entry, `HumanName` |
| New layout                         | `Task not serializable`: `NotSerializableException: ListTraceCollector` | passes, 1 entry, `HumanName` |

So the collector captures the same FHIR type on both layouts, and nothing about
the new layout's structures is unserialisable. The test simply cannot run on any
data except a local relation.

- **The six errors are one cause.** The first reports
  `ExceptionInInitializerError` from `SerializationDebugger$.<clinit>`. That is an
  `IllegalAccessException` on `sun.security.action.GetBooleanAction`, because
  the surefire `argLine` does not open `java.base/sun.security.action`. The other
  five report `NoClassDefFoundError` for the same class, which follows from the
  first. `evaluationWithoutCollector_stillWorks`, the only case without a
  collector, passes.
- **Proposed resolution, kind (c).** In `CollectorTests.setUpCollector`, pass
  `TraceCollectorProxy.create(collector)` to `withTraceCollector`, and close the
  proxy in an `@AfterEach`. The assertions do not change. This is what
  `SingleInstanceEvaluator` does, and the probe shows it passes on all three
  kinds of data.
- **Rejected: making `ListTraceCollector` serialisable.** The executor would
  add entries to its deserialised copy, and the driver's list would stay empty.
- **Rejected: wrapping inside the builder.** The proxy has to be closed after
  materialisation, and the builder does not know when that is.
- **Optional, and not a test change.** Adding
  `--add-opens=java.base/sun.security.action=ALL-UNNAMED` to the surefire
  `argLine` would make every future serialisation failure report
  `Task not serializable` instead of this misleading error. A `TraceProbe` run
  with that flag, and without `extraDebugInfo=false`, confirms it. It reported
  `Task not serializable`, caused by `NotSerializableException:
ListTraceCollector`.

## 7–8. The trace entry count, #2594

These two cases compare a string with an integer. The types are not
equivalent, so `EqualityOperator.handleNonEquivalentTypes` builds:

```
CASE WHEN (isnull(trace(a, t, string, null)) OR isnull(1)) THEN null ELSE false END
```

`TraceExpression.nullable` returns `child.nullable`. A literal is not nullable,
so `NullPropagation` rewrites `isnull(trace(...))` to `false`, and the `CASE`
simplifies to `false`. The trace is gone from the plan, even though the
expression is `Nondeterministic`, and so it never fires. `PlanProbe` shows both
optimised plans. Over Parquet it is `Project [id#79, false AS value#108]`. Over
the local relation, the optimiser's early `ConvertToLocalRelation` batch has
already evaluated the projection by then, and the trace fires once, during
optimisation.

`TraceProbe` counts the entries through `SingleInstanceEvaluator`, the path the
test uses:

| Expression                                         | Local relation | Previous layout from Parquet | New layout |
| -------------------------------------------------- | -------------: | ---------------------------: | ---------: |
| `'a'.trace('t')`                                   |              1 |                            1 |          1 |
| `'a'.trace('t') = 1`                               |              1 |                        **0** |      **0** |
| `'a' = 1.trace('t')`                               |              1 |                        **0** |      **0** |
| `'a'.trace('t') = 'a'`                             |              1 |                            1 |          1 |
| `Patient.name.family.first().trace('t') = 'Smith'` |              1 |                            1 |          1 |

- **This is a real engine defect, and it has nothing to do with the layout.** Any
  table read from storage loses the trace. The expected count of 1 is correct:
  the operand is evaluated, so `trace()` should report it. The test's comment
  names this very case as a guard for #2594.
- **Proposed resolution, kind (a).** In `encoders/.../TraceExpression.scala`,
  `TraceExpression` and `TraceProjectionExpression` report `nullable = true`, so
  the optimiser cannot prove the traced value non-null and drop it.
    - No test changes.
    - The previous layout's counts stay as they are: the local relation still
      evaluates everything.
    - The change widens the nullability of a traced column in its schema, from
      non-null to nullable. A default run must confirm that no existing test
      asserts that nullability.
    - It was not made here, because it is not a new-layout bug.

## 9–10. `instant`

Both cases read `Observation.issued`, stored from
`new InstantType("2023-01-01T12:00:00+10:00")`. Neither column declares a FHIR
type, so T111's cast does not apply. On the previous layout the encoder stores
`instant` as `TimestampType`. On the new layout it is text in its lexical form
(data-model, and decision 70's input contract, which accepts no `TimestampType`
for the temporal types). No port step covered it, because decision 47's four
discriminators do not include temporal types.

`InstantProbe` shows what the engine does on each layout:

| Expression or column                                               | Previous layout                          | New layout                                |
| ------------------------------------------------------------------ | ---------------------------------------- | ----------------------------------------- |
| stored type                                                        | `TimestampType`                          | `StringType`                              |
| `issued`                                                           | `TimestampType`, `2023-01-01 02:00:00.0` | `StringType`, `2023-01-01T12:00:00+10:00` |
| `issued.toString()`                                                | `2023-01-01 02:00:00`                    | `2023-01-01T12:00:00+10:00`               |
| `issued = @2023-01-01T02:00:00Z`                                   | `true`                                   | `true`                                    |
| `issued = @2023-01-01T12:00:00+10:00`                              | `true`                                   | `true`                                    |
| `issued > @2023-01-01T01:30:00Z`, `issued < @2023-01-01T02:30:00Z` | `true`, `true`                           | `true`, `true`                            |
| view column, no type (case 9)                                      | `TimestampType`, `2023-01-01 02:00:00.0` | `StringType`, `2023-01-01T12:00:00+10:00` |
| view column, declared `instant`                                    | `StringType`, `2023-01-01 02:00:00`      | `StringType`, `2023-01-01T12:00:00+10:00` |
| view column, `TIMESTAMP WITH TIME ZONE`                            | `TimestampType`, `2023-01-01 02:00:00.0` | `TimestampType`, `2023-01-01 02:00:00.0`  |
| view column, `TIMESTAMP WITHOUT TIME ZONE` (case 10)               | `TimestampNTZType`, `2023-01-01T02:00`   | `TimestampNTZType`, `2023-01-01T12:00`    |

- **FHIRPath comparison agrees across the layouts.** The divergence is in what
  leaves the engine. That includes two surfaces no existing test covers:
  `toString()` and a declared `instant` column. Both render the previous
  layout's value in UTC without the offset, and the new layout's value in its
  lexical form.
- **Case 10 is wrong on the new layout.** `TIMESTAMP WITH TIME ZONE`
  (`miscAnsiCasts[3]`) passes, because Spark honours the offset when it casts a
  string to `TIMESTAMP`. For `TIMESTAMP_NTZ` it drops the offset and keeps the
  wall time. An instant is a point in time, so its value without a time zone is
  the session-time value, 02:00, which is what the previous layout gives.

The owner decides what the query-time type of `instant` is. There are two
options.

- **Option A: `instant` is `TimestampType` at query time on both layouts.** The
  new layout decodes the text to a timestamp at traversal, as decimals are
  decoded to `DECIMAL(32,6)`.
    - It changes no existing test and fixes both cases, so it is kind (a).
    - It brings `toString()` and the declared column onto the previous layout's
      UTC rendering, which loses the source offset.
    - It adds a normalisation branch that runs in the opposite direction to the
      others, from the new layout to the previous layout's type.
- **Option B: `instant` is text at query time, like `dateTime`.** This keeps the
  new layout's answer, which preserves the lexical form.
    - It needs one test change, of kind (c): `defaultMiscMappings[6]` would expect
      `StringType` and `2023-01-01T12:00:00+10:00`.
    - It needs one main-code fix, of kind (a), for case 10. An `ansi/type` of
      `TIMESTAMP WITHOUT TIME ZONE` over a collection whose FHIR type is
      `instant` would be cast through `TIMESTAMP` first, so that the offset is
      applied. `DateTimeCollection` already carries `FHIRDefinedType.INSTANT`
      for this purpose.
    - **The fix must be scoped to `instant`.** A string of any other type keeps
      its wall time, and existing tests pin that.
      `AnsiTypeHintingTest.ansiLegalCasts`, and through it
      `legalCollectionAnsiCasts`, expects `TIMESTAMP WITHOUT TIME ZONE` over the
      string `2023-01-01T12:00:00-02:00` to give `2023-01-01T12:00`. So does
      `TIMESTAMP(3)` over `+02:00`. A `dateTime`, which is text on both
      layouts, keeps the wall time as well.
    - With that fix, `miscAnsiCasts[2]` is unchanged. The previous layout's
      answers are unchanged too: its `instant` is already a `TimestampType`, so
      the extra cast does nothing, and nothing else is affected. The scoped cast
      has not been tried.
    - `DateTimeCollection.fromValue(InstantType)`, which builds a timestamp
      literal, and `asStringPath`, which formats a timestamp, would then need
      checking against text.

**Recommendation: option B.** It keeps the fidelity the new layout was built for,
and it removes the engine's one timestamp-typed FHIR primitive rather than
adding a branch to preserve it. Option A is the choice if M2's rule of no
existing test changes should outweigh that. The previous layout's answers stay
as they are until T100e under either option.

## 11–13. The conformance baselines

All three are "excluded test passed" reports, or the equivalent for case 12,
where the case fails with an assertion rather than the recorded error. Each one
comes from a gap that belongs to the encoder, and the new layout does not have
it:

- **`StructureDefinition`.** `ResourceTypes.UNSUPPORTED_RESOURCES` makes the
  encoder refuse it. `SdProbe` shows the previous-layout fixture failing with
  `UnsupportedResourceError: Encoding is not supported for resource:
StructureDefinition`, and the new-layout transform reading the resource. The
  spec does not say whether the new layout is meant to support the resource
  types the encoder refuses. The transform reads it because the definitions
  describe it and the fitted schema has no recursion bound. **Owner question**: if
  `StructureDefinition` is to stay unsupported on the new layout, the refusal
  belongs at the source boundary instead (M4), and the exclusion stays as it
  is.
- **`ValueSet.expansion.contains`.** The encoder skips the recursive field
  (`R4DataTypeMappings.skipField`), and the transform keeps it.

The previous layout stays available as an opt-in run after T100g, and T100i
puts that run in CI from M4. So removing these exclusions would turn the
previous-layout run red. Each rule has to apply to the previous layout only
instead.

- **The mechanism.** A rule's matchers are OR-ed, so the condition has to be a
  single SpEL predicate. `SpELPredicate` evaluates with a
  `StandardEvaluationContext`, which allows static type references. `SpelProbe`
  shows the predicate below is `true` under `previous` and `false` under `pof`.
  It has not been run inside `YamlReferenceImplTest`, because that would mean
  editing the baseline.
- **The first matching rule wins.** The #2418 rule is the first in its block, so
  once it stops matching under `pof`, case 12 falls through to the #2524 rule
  further down.

The concrete change to `fhirpath/src/test/resources/fhirpath-js/config.yaml`,
proposed and not made:

```yaml
      # "Unsupported resources" (#2418): the regex matcher becomes a SpEL
      # predicate, so that the rule applies to the previous layout only.
        spel:
          - "#testCase.expression matches '^StructureDefinition.*' and
            !T(au.csiro.pathling.test.layout.TestLayout).active().isPof()"

      # "Type hierarchy checking not yet implemented" (#2524): one more entry.
          - "StructureDefinition.snapshot.element.type is Element"

      # "Recursive ValueSet.expansion.contains not encoded": the `any` matcher
      # becomes a SpEL predicate, scoped the same way.
        spel:
          - "#testCase.expression == 'ValueSet.expansion.repeat(contains).count() = 10' and
            !T(au.csiro.pathling.test.layout.TestLayout).active().isPof()"
```

The rules' comments would say that they apply to the previous layout only, and
why. #2418 and #2524 are both open issues.

## Approvals needed

Decision 73 requires the owner's approval for each of these before it is made.
**All five were approved by the owner on 2026-09-28**, together with option B
for `instant` (decision 81) and the new layout reading `StructureDefinition`.

The owner revised the `instant` decision on the same day. Option B stands for the
new layout. In addition, the previous layout's timestamp is normalised to its
text in UTC at read time (decision 81), so the engine sees text instants on both
layouts. Approval 2 was revised to match. The row now expects `StringType` on
both layouts, with `2023-01-01T12:00:00+10:00` under `pof` and
`2023-01-01T02:00:00Z` under `previous`. Before the normalisation was made, the
tests of the other modules were checked:

- the `library-api` suite was run, 168 tests, all green;
- the Python and R tests were searched, and none asserts an instant's type or
  value;
- the server reads `meta.lastUpdated` directly, not through the engine.

No other existing test changed. The previous layout stores no FHIR type other
than `instant` as a timestamp: in `R4DataTypeMappings`, `InstantType` is the only
primitive mapped to `TimestampType`.

Approvals 3 to 5 were checked in the runner on both
layouts: under `pof`, `testTypes[27]` is excluded by #2524 and `[28]` and
`testFhirR4[218]` pass unexcluded. Under `previous`, `[27]` and `[28]` are
excluded by #2418 and `[218]` by the `contains` rule.

| #   | Test or exclusion                                                                                                               | Kind        | The change                                                                                                                                                                                                                                                 |
| --- | ------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `TraceFunctionTest$CollectorTests`, all 7 cases, through the shared `setUpCollector`                                            | (c)         | `.withTraceCollector(collector)` becomes `.withTraceCollector(proxy)`, where `proxy = TraceCollectorProxy.create(collector)`, and a new `@AfterEach` calls `proxy.close()`. The assertions are unchanged.                                                  |
| 2   | `AnsiTypeHintingTest.defaultMiscMappings[6]`, the "instant" row of `miscDefaultMappings`, as revised                            | (c)         | `Arguments.of("instant", "issued", false, DataTypes.TimestampType, "2023-01-01 02:00:00.0")` becomes `Arguments.of("instant", "issued", false, DataTypes.StringType, TestLayout.active().isPof() ? "2023-01-01T12:00:00+10:00" : "2023-01-01T02:00:00Z")`. |
| 6   | `encoders` `TraceExpressionTest.traceDataTypeAndNullableDelegateToChild` and `traceProjectionDataTypeAndNullableDelegateToLeft` | (c)         | `assertFalse(expr.nullable())` becomes `assertTrue(expr.nullable())`. The tests are renamed `traceDataTypeDelegatesToChildAndIsAlwaysNullable` and `traceProjectionDataTypeDelegatesToLeftAndIsAlwaysNullable`, and keep their `dataType` checks.          |
| 3   | `fhirpath-js/config.yaml`, rule "Unsupported resources" (#2418)                                                                 | (b), narrow | `expression: ["^StructureDefinition"]` becomes the SpEL predicate above, which applies to the previous layout only. Covers `testTypes[27]` and `[28]` under `pof`.                                                                                         |
| 4   | `fhirpath-js/config.yaml`, rule "Type hierarchy checking not yet implemented" (#2524)                                           | (b), add    | `StructureDefinition.snapshot.element.type is Element` is added to `any`, with the rule's existing `outcome: failure`. It applies only under `pof`, where #3 no longer matches first.                                                                      |
| 5   | `fhirpath-js/config.yaml`, rule "Recursive ValueSet.expansion.contains not encoded"                                             | (b), narrow | `any: ["** testRepeat1"]` becomes the SpEL predicate above, which applies to the previous layout only. Covers `testFhirR4[218]`.                                                                                                                           |

The two owner decisions, made on 2026-09-28:

- **The query-time type of `instant`** is text on both layouts: option B, with
  the previous layout normalised at read time (decision 81).
- **The new layout reads `StructureDefinition`.**

The main-code fixes of kind (a):

- **`TraceExpression` and `TraceProjectionExpression` become nullable** (cases
  7–8). This fix does change existing tests, contrary to what this record first
  said. `TraceExpressionTest.traceDataTypeAndNullableDelegateToChild` and
  `traceProjectionDataTypeAndNullableDelegateToLeft`, in `encoders`, assert
  `assertFalse(expr.nullable())` over a literal child. They predate the branch.
  The change was approved separately, as approval 6, and is made.
- **The `instant` fix for cases 9–10** has two parts. The first is the
  `TIMESTAMP WITHOUT TIME ZONE` cast, scoped to `instant`. The second is the
  read-time normalisation of the previous layout's instant (decision 81),
  covered by `InstantNormalisationTest`. Both are made. No existing test
  changes beyond approval 2.
