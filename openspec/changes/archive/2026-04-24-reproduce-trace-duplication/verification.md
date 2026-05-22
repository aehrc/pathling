# Verification

## Default test run (known-failing tests excluded)

Command:

```
mvn test -pl fhirpath -Dtest=TraceFunctionTest -Dsurefire.failIfNoSpecifiedTests=false
```

Result: `BUILD SUCCESS`. Summary across both Surefire executions:

- `TraceFunctionTest$TraceEntryCountTest`: 4 passed, 0 failed
- `TraceFunctionTest$CollectorTests`: 7 passed, 0 failed
- `TraceFunctionTest$ErrorTests`: 1 passed, 0 failed
- `TraceFunctionTest$LoggingTests`: 6 passed, 0 failed
- `TraceFunctionTest$PassThroughTests`: 12 passed, 0 failed
- Total: 30 tests, 0 failures

The 6 known-failing rows are tagged and excluded from default.

## On-demand known-failing run (reproducing the bug)

Command:

```
mvn test -pl fhirpath -Dtest=TraceFunctionTest \
    -Dpathling.test.excludedGroups=none -Dgroups=known-failing \
    -Dsurefire.failIfNoSpecifiedTests=false
```

Result: `BUILD FAILURE` with 6 tests run, 6 failures, 0 errors.

### Observed failures

Every row below is an `AssertionFailedError` with the expected value derived from issue #2594:

| Expression                                                               | Expected | Actual | Ratio |
| ------------------------------------------------------------------------ | -------- | ------ | ----- |
| `Patient.name.trace('t').given.count()`                                  | 3        | 6      | 2×    |
| `Patient.name.trace('t').exists()`                                       | 3        | 12     | 4×    |
| `Patient.name.trace('t').empty()`                                        | 3        | 6      | 2×    |
| `Patient.name.trace('t').given.join(' ').combine('X')`                   | 3        | 6      | 2×    |
| `Patient.name.trace('t').given.join(' ') \| Patient.name.family.first()` | 3        | 6      | 2×    |
| `Patient.name.trace('t') \| Patient.name.trace('t')`                     | 6        | 12     | 2×    |

## Matrix rows from #2594 that did not reproduce in this path

- `Patient.name.trace('t').first()` — returned 3 (expected). `first()` does not
  duplicate in the `SingleInstanceEvaluator` code path used here. Included in
  the passing-case regression guard so any fix must keep it passing.
- `Patient.name.trace('t').last()` — threw
  `UnsupportedFhirPathFeatureError: Unsupported function: last`. Pathling does
  not implement `last()`; row omitted from the test suite.

## Environment

- Pathling main branch (commit at time of verification: see `git log -1`)
- Spark 4.0.2, Scala 2.13.16, Java 21
- Surefire 3.2.5
- Spring Boot unit-test profile
