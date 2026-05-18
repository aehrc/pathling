## 1. Calibration

- [x] 1.1 ~~Record baseline~~ — **superseded**: during implementation we switched to using `SingleInstanceEvaluator` (the FHIRPath Lab API path), which produces trace counts that match the issue matrix exactly (3 entries per name-element for a 3-name patient). Absolute counts from the issue matrix are used directly.
- [x] 1.2 ~~Recompute expected values~~ — **superseded** by 1.1. Absolute counts from the #2594 matrix used directly.
- [x] 1.3 Verified: Surefire 3.2.5 with JUnit 5 tag filtering. The default-test and sof-compliance-test Surefire executions both set `<excludedGroups>${pathling.test.excludedGroups}</excludedGroups>`, defaulted to `known-failing` via a project property. To run known-failing on demand: `-Dpathling.test.excludedGroups=none -Dgroups=known-failing`.

## 2. Test fixture

- [x] 2.1 Added `createPatientWithThreeNames()` in `TraceFunctionTest` with the exact #2594 fixture (three names: `use=official,family=Smith,given=[John,Quincy]`; `use=usual,family=Smith,given=[Johnny]`; `use=maiden,family=Doe,given=[John,Q]`). Header comment references the issue.
- [x] 2.2 The per-case helper (`countTraceValues`) builds a fresh `SingleInstanceEvaluator.evaluate(...)` call per invocation. No shared-evaluator state to reset.

## 3. Test class

- [x] 3.1 New `@Nested class TraceEntryCountTest` inside `TraceFunctionTest`. Matches the convention used by other nested classes (`PassThroughTests`, `CollectorTests`, etc.).
- [x] 3.2 Defined `record TraceEntryCase(String expression, String label, int expected)`. Changed from the original plan's `multiplier` field to a direct `expected` field since we observed counts that match the #2594 matrix exactly (no ratio calculation needed).
- [x] 3.3 Implemented `passingEntryCountCases()` (4 rows — includes `.first()` since it does NOT duplicate in this path) and `knownFailingEntryCountCases()` (6 rows — drops `.last()` which is unsupported in Pathling).
- [x] 3.4 Implemented `entryCount_nonDuplicatingOperations` (untagged) and `entryCount_duplicatingOperations_bug2594` (`@Tag("known-failing")`). The split-method approach avoids the JUnit-5-per-parameter tag issue.
- [x] 3.5 Assertion message includes expression, expected count, label, actual count, and a `See issue #2594.` pointer.

## 4. Build-side configuration

- [x] 4.1 Added `<excludedGroups>${pathling.test.excludedGroups}</excludedGroups>` to both Surefire executions in `fhirpath/pom.xml` (default-test and sof-compliance-test). Property defaulted to `known-failing` in the fhirpath POM `<properties>` section.
- [x] 4.2 Confirmed on-demand invocation works via `-Dpathling.test.excludedGroups=none -Dgroups=known-failing`. Documented in the test class header.

## 5. Local verification

- [x] 5.1 Default run: `mvn test -pl fhirpath -Dtest=TraceFunctionTest` → 30 tests, 0 failures (4 TraceEntryCountTest passing + 26 pre-existing). Known-failing rows correctly skipped.
- [x] 5.2 On-demand run: 6 tests run, 6 failures — all 6 duplicating operations reproduce the bug with the expected actual counts. See `verification.md` for the full matrix.

## 6. Final checks

- [x] 6.1 `openspec validate reproduce-trace-duplication --strict` passes (to be re-verified after task updates).
- [x] 6.2 `git diff --stat` shows only: `fhirpath/pom.xml`, `fhirpath/src/test/java/.../TraceFunctionTest.java`, and `openspec/changes/reproduce-trace-duplication/**`. No production code modified.
