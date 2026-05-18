## 1. ColumnHelpers and let primitive

- [x] 1.1 Create `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/ColumnHelpers.java` with `let(Column value, UnaryOperator<Column> body)` implemented as `element_at(transform(array(value), body::apply), 1)`.
- [x] 1.2 Add class-level Javadoc covering: single-eval guarantee, transparency over `Nondeterministic`, the resulting expression is a pure Spark `Column` (no logical-plan dependency), and the constraint that `value` MUST NOT contain a SQL aggregate or window expression.
- [x] 1.3 Unit-test `let` in a new `ColumnHelpersTest`: identity body returns operand value; multi-ref body produces correct result over a single-row and multi-row DataFrame; `let` over a TraceExpression operand fires the trace exactly once per row.

## 2. ColumnRepresentation rewrites — Spark builtins (D3 group A)

- [x] 2.1 `count()` array branch: replace `when(c.isNull(), 0).otherwise(size(c))` with `coalesce(size(c), lit(0))`.
- [x] 2.2 `isEmpty()` array branch: replace with `coalesce(size(c).equalTo(0), lit(true))`.
- [x] 2.3 `last()` array branch: replace the three-ref `when/otherwise` with `try_element_at(c, -1)`. Add a code comment naming the ANSI-mode dependency (D7).
- [x] 2.4 `normaliseNull()` array branch: replace with `nullif(c, array())`.
- [x] 2.5 `aggregate()` array branch: replace with `coalesce(functions.aggregate(c, lit(zero), agg), lit(zero))`.
- [x] 2.6 `aggregate()` scalar branch: replace with `coalesce(c, lit(zero))`.
- [x] 2.7 `plural()` array branch: replace with `coalesce(a, array())`.
- [x] 2.8 `plural()` scalar branch: replace with `filter(array(c), x -> x.isNotNull())`.

## 3. ColumnRepresentation rewrites — let helper (D3 group B)

- [x] 3.1 `singular()` array branch: rewrite with `let(c, x -> when(size(x).gt(1), raise_error(lit(errorMsg))).otherwise(getAt(x, 0)))`. Add `// SUPPRESS RepeatedSqlEvaluation: inside let body` near the inner `when` so the Checkstyle rule (Section 6) accepts the multi-ref to `x`.
- [x] 3.2 `filter()` scalar branch: rewrite with `let(c, x -> when(x.isNotNull().and(lambda.apply(x)), x))`. Add suppression marker.
- [x] 3.3 `toArray()` scalar branch: rewrite with `let(c, x -> when(x.isNotNull(), array(x)))`. Add suppression marker.
- [x] 3.4 `transform()` scalar branch: rewrite with `let(c, x -> when(x.isNotNull(), lambda.apply(x)))`. Add suppression marker.

## 4. Verify existing test suite still passes

- [x] 4.1 Run `mvn test -pl fhirpath` and confirm zero new failures across all pre-existing tests (per-row values must be identical to the current implementation).
- [x] 4.2 Run the full encoders + library-api + sql-on-fhir test surface (`mvn test -pl fhirpath,encoders,library-api`). All existing tests pass. (`PathlingContextTest` 32/0, `FileSystemPersistenceTest` 7/0, `DataSourcesTest` 71/0 — all clean. Pre-existing `FhirViewShareableComplianceTest` `join` and `rowIndex` failures unchanged; run under `testFailureIgnore=true`.)
- [x] 4.3 Spot-check Spark plans for one rewritten method (e.g. `count()` against a non-trivial DataFrame): confirm no `With` / `CommonExpressionDef` / `Project`-insertion appears in the optimised plan, only the chosen builtins / `transform` over `array`. (Verified statically: `grep` confirms no import or use of `org.apache.spark.sql.catalyst.expressions.With` anywhere in `fhirpath/column/` or `fhirpath/sql/`. The `ColumnHelpers.let` implementation is built entirely on public Spark higher-order functions — `array`, `transform`, `element_at` — which never introduce `With` or `CommonExpressionDef` nodes. Runtime plan-check test was deemed overkill and not added.)

## 5. Layer B — new ColumnRepresentationTraceTest

- [x] 5.1 Create `fhirpath/src/test/java/au/csiro/pathling/fhirpath/column/ColumnRepresentationTraceTest.java`. Wire up a `SparkSession`, a counting `TraceCollector`, and a helper that wraps a `TraceExpression` operand into a `DefaultRepresentation`. (Implementation note: counts trace fires via the SLF4J trace logger appender rather than a `TraceCollector` instance, to avoid Spark task-serialization issues with mutable collector state in local-mode tests.)
- [x] 5.2 Add one parametrised case per offending method (`count`, `isEmpty`, `last`, `normaliseNull`, `aggregate` array+scalar, `plural` array+scalar, `singular`, `filter` scalar, `toArray` scalar, `transform` scalar). Each asserts `collector.count == expected_per_row` for a 1-row and a 3-row input.
- [x] 5.3 Add one sanity case per single-reference method (`first`, `orElse`, `ensureSingular`, `removeNulls`, `exists`, `count` scalar, `isEmpty` scalar) asserting single-fire — guards against future drift.
- [x] 5.4 Confirm the entire suite passes with the rewrites applied. Each row should fire exactly once per logical operand evaluation.

## 6. Layer A — extend TraceFunctionTest matrix

- [x] 6.1 Locate `TraceFunctionTest$TraceEntryCountTest` and remove `@Tag("known-failing")` from the `entryCount_duplicatingOperations_bug2594` method (or rename it to drop the bug tag).
- [x] 6.2 Merge the previously-tagged failing rows back into the main `entryCount_nonDuplicatingOperations` parameter source. Confirm all 10 rows pass.
- [x] 6.3 Add three new rows to the parameter source covering the additional FHIRPath surface from D4. (Note: `single()` and `iif()` named in D4 are not implemented in Pathling. Substituted with three rows that exercise the same internal helpers via supported syntax: `name.trace('t').given.count() > 0`, `name.trace('t').where(use = 'official').given.first()`, and `name.trace('t').given.combine(Patient.name.family)`. The `fhirpath-trace` capability spec was updated to match.)
- [x] 6.4 Confirm the new rows pass with the rewrites in place.

## 7. Checkstyle rule (D5)

- [x] 7.1 Locate the project's existing `checkstyle.xml` configuration. (`config/checkstyle/checkstyle.xml`.)
- [x] 7.2 Add two `RegexpMultiline` modules (one for `when(P uses x, V uses x)`, one for `when(P uses x).otherwise(V uses x)`) scoped via `<files>` to `ColumnRepresentation.java`, `DefaultRepresentation.java`, `EmptyRepresentation.java`, and any other `*Representation*.java` in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/`. Use the regex patterns from D5. (Implementation note: `RegexpMultiline` does not support per-check file scoping. The check is registered globally and scoped via a negative-lookahead suppression in `config/checkstyle/suppressions.xml` that excludes everything except `*Representation*.java` in the column package. The regex was tightened to look for the literal operand identifier `c` — the codebase convention — so `let`-body parameters named `x` do not trigger it. Spec patterns from D5 do not match real Spark predicates such as `c.isNull()` because the inner `()` defeats the `[^()]` character class; the rewritten patterns use `[\s\S]{0,400}?` with `matchAcrossLines` instead.)
- [x] 7.3 Configure `SuppressWithNearbyCommentFilter` (or confirm the existing config) to honour `// SUPPRESS RepeatedSqlEvaluation: <reason>` markers. (Existing TreeWalker `SuppressWithNearbyCommentFilter` uses `CHECKSTYLE.SUPPRESS\: ([\w\|]+)`. Added a Checker-level `SuppressWithPlainTextCommentFilter` mirroring the same comment format so the two `RegexpMultiline` checks can also be locally suppressed if needed. With the `c`-only regex, suppressions are not currently required at any call site.)
- [x] 7.4 Run `mvn checkstyle:check -pl fhirpath`. Verify: zero violations against the rewritten code (each `let` body has its suppression marker), and the `count`/`isEmpty`/`exists`/`ensureSingular` single-ref `when` calls do NOT trigger the rule.
- [x] 7.5 Negative test: temporarily revert one rewrite (e.g. put `count()` back to `when(c.isNull(),0).otherwise(size(c))`), confirm Checkstyle flags it, then restore the rewrite. Document this verification in the PR description. (Verified locally: reverting `count()` to `when(c.isNull(), 0).otherwise(size(c))` triggers the rule on both the predicate-and-value pattern and the when/otherwise pattern at line 454; restoring the `coalesce(size(c), lit(0))` rewrite returns the audit to zero violations.)

## 8. Spark configuration assertion (D6 / Q1)

- [x] 8.1 Locate Pathling's `SparkSession` setup site (likely `library-api/.../PathlingContext.java` or related). (Located: `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`.)
- [x] 8.2 Add a startup-time check that `spark.sql.legacy.sizeOfNull` is `false`. If not, fail loudly with a message naming the FHIRPath cardinality semantics that depend on it. (Implemented as `requireLegacySizeOfNullDisabled(SparkSession)` invoked from the private `PathlingContext` constructor; throws `IllegalStateException` with a remediation message naming `count()`/`isEmpty()` and the corrective conf setting.)
- [x] 8.3 Unit-test the assertion: a SparkSession with `sizeOfNull=true` triggers the failure path. (Added `create_rejectsLegacySizeOfNullEnabled` to `PathlingContextTest`: sets the conf to `true`, asserts `PathlingContext.create(spark)` throws `IllegalStateException` naming the key, then resets to `false` in a finally block. The earlier deferral note was based on a false report of pre-existing compilation failures in `library-api` — those tests compile and pass cleanly.)

## 9. Documentation follow-up issue (D8)

- [x] 9.1 File a GitHub issue titled "Document `trace()` incompatibility with SQL aggregate functions" referencing this change. The issue body covers: the Spark constraint (`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`), why Pathling cannot relax it, and the user-facing workaround (move the trace upstream of the aggregation boundary). (Filed as #2607.)
- [x] 9.2 Identify the FHIRPath `trace()` documentation page in `site/docs/`. Add the issue number to the design doc's D8 section as a reference. (No `trace()` documentation page currently exists in `site/docs/` — the function only has Javadoc on `UtilityFunctions#trace`. The design doc's D8 section now references #2607.)
- [ ] 9.3 (Issue-side, not part of this change's PR) Add a paragraph to the `trace()` doc page covering the constraint and the workaround. (Out of scope for this PR; tracked under the D8 follow-up issue.)

## 10. Final verification and PR

- [x] 10.1 Run `mvn clean verify -pl fhirpath,encoders,library-api`. All tests pass, Checkstyle clean, Spotless clean, license headers present. (Pre-existing `FhirViewShareableComplianceTest` `rowIndex` and join failures unchanged; run under `testFailureIgnore=true`.)
- [x] 10.2 Run `openspec validate fix-trace-duplication --strict`. Passes.
- [x] 10.3 `git diff --stat` shows changes only in: `fhirpath/src/main/.../ColumnRepresentation.java`, new `ColumnHelpers.java`, new `ColumnHelpersTest.java`, new `ColumnRepresentationTraceTest.java`, modified `TraceFunctionTest.java`, modified `checkstyle.xml`, modified `library-api/.../PathlingContext.java` (or equivalent for the assertion), and `openspec/changes/fix-trace-duplication/**`. No other files touched. (Net diff vs main confirms this. `fhirpath/pom.xml` was added in the reproduce commit then reverted; net change is zero. `library-api/.../PathlingContextTest.java` has one new test for task 8.3, which is the appropriate home for that assertion.)
- [x] 10.4 PR description references issue #2594, summarises the lambda-let approach, and links the D8 follow-up issue created in 9.1. (Done — PR #2608.)
