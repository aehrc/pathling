## Context

Issue #2594 describes a bug where `trace()` collector entries are duplicated
when the traced column is consumed by operations that compile into
`when(cond(c), …).otherwise(expr(c))` patterns in `ColumnRepresentation`.
Examples include `count()`, `exists()`, `empty()`, `first()`, `last()`,
`combine()`, and `|` (union via `plural()`).

Prior investigation confirmed (against Spark 4.0.2 in
`spark-catalyst_2.13-4.0.2-sources.jar`):

- `TraceExpression` is `Nondeterministic`, so Catalyst's CSE excludes it.
- Even if `TraceExpression` were made deterministic, Spark's CSE is
  conservative around `CaseWhen`: `EquivalentExpressions.childrenToRecurse`
  only walks `alwaysEvaluatedInputs` and cross-compares `branchGroups`. A
  subexpression appearing once in the always-evaluated predicate AND once in
  a single conditional branch is NOT registered as a common subexpression.
- Therefore the duplication is observable regardless of determinism, and a
  fix requires either rewriting the affected `ColumnRepresentation` patterns
  or intercepting trace evaluation at runtime.

This change adds a test suite that pins the bug down. It does not fix the
bug.

The existing test class `TraceFunctionTest` is the home for trace tests; it
already has `ListTraceCollector` wiring via `EvaluationContext`. Adding a new
`@Nested` class keeps the new tests discoverable and grouped.

## Goals / Non-Goals

**Goals:**

- Produce an executable, parametrised test suite that encodes every row of
  the reproduction matrix in #2594.
- Make the test suite serve as the acceptance oracle for the subsequent fix
  change: when the fix lands, the suite SHALL pass in full.
- Use a fixture that matches the issue (one Patient, three `name` entries)
  so doubled / tripled counts are unambiguous.
- Keep currently-passing cases enabled as regression guards so a fix does
  not accidentally break them.

**Non-Goals:**

- Fixing the duplication bug. That is a separate change that will consume
  the test suite as its acceptance criteria.
- Modifying `TraceExpression`, `ColumnRepresentation`, `CombiningLogic`, or
  any other production code.
- Changing the public API or any user-facing behaviour.
- Adding tests for any trace behaviour not related to entry-count fidelity
  (pass-through, log output, projection semantics — those are already
  covered in the existing test class).

## Decisions

### D1. Fixture: a single Patient with three names

Match the issue exactly:

```json
{
    "resourceType": "Patient",
    "name": [
        { "use": "official", "family": "Smith", "given": ["John", "Quincy"] },
        { "use": "usual", "family": "Smith", "given": ["Johnny"] },
        { "use": "maiden", "family": "Doe", "given": ["John", "Q"] }
    ]
}
```

Three elements is the minimum that distinguishes correct counts (3) from
doubled (6) from tripled (9). Two elements would collapse the doubled
(4) and a baseline drift with a random off-by-one into visually similar
numbers.

**Alternative considered:** reuse the existing test Patient fixture used by
other `TraceFunctionTest` nested classes. Rejected: the existing fixture may
not have three distinct `name` entries, and using a shared fixture couples
these tests to unrelated changes in the common setup. A local fixture keeps
the tests hermetic and readable.

### D2. Parametrisation: one test method, one `@MethodSource`

Use a `record TraceEntryCase(String expression, String label, int expected)`
and a `@ParameterizedTest` with `@MethodSource` that yields the 11 matrix
rows. Each row renders as a distinct test name in JUnit's output.

**Alternative considered:** 11 separate `@Test` methods. Rejected:
boilerplate, and harder to read as a table. Parametrisation better expresses
"this is a matrix, here are its rows."

### D3. Assertion: count entries by label

The assertion is:

```
collector.getEntries().stream()
  .filter(e -> e.label().equals(case.label()))
  .count() == case.expected()
```

This filters by the expected label so a single test case can isolate one
trace even when the expression contains multiple. It also means the
assertion is robust to the exact evaluation order of unrelated trace calls.

**Alternative considered:** assert the total `getEntries().size()`.
Rejected: the union case `name.trace('t') | name.trace('t')` is expected to
produce 6 entries from TWO traces with the same label. Filtering by label
isn't strictly necessary in the issue's matrix (same label throughout) but
keeps the helper reusable if we extend the matrix later.

### D4. Handle known-failing cases without `@Disabled`

Three options considered:

1. `@Disabled("fixed in #TBD")` — the tests don't run at all. Rejected: the
   point of adding them is to have a red signal in CI.
2. Tag known-failing cases with `@Tag("known-failing")` and exclude that tag
   from the default Surefire run. Tests still compile, can be run on demand
   with `-Dgroups=known-failing`.
3. Assert the current (buggy) counts and flip them when the fix lands.
   Rejected: encodes the bug into the test, loses the documentation value,
   and requires a two-sided change at fix time.

**Decision:** option 2. Tag known-failing rows at the parameter-source level
so JUnit's `@Tag` can filter them. If Surefire configuration does not allow
per-parameter tagging cleanly, fall back to splitting the matrix into two
methods: one for currently-passing rows, one for currently-failing rows
marked `@Tag("known-failing")`. Tasks.md will call out which form to use
based on a quick spike.

The subsequent fix change will remove the tag and the split.

### D5. Explicit row-by-row expected counts

The matrix from the issue is reproduced verbatim with the expected counts:

| Expression                                               | Expected |
| -------------------------------------------------------- | -------- |
| `name.trace('t')`                                        | 3        |
| `name.trace('t').given.join(' ')`                        | 3        |
| `name.trace('t').given.join(' ') + 'X'`                  | 3        |
| `name.trace('t').given.count()`                          | 3        |
| `name.trace('t').exists()`                               | 3        |
| `name.trace('t').empty()`                                | 3        |
| `name.trace('t').first()`                                | 3        |
| `name.trace('t').last()`                                 | 3        |
| `name.trace('t').given.join(' ').combine('X')`           | 3        |
| `name.trace('t').given.join(' ') \| name.family.first()` | 3        |
| `name.trace('t') \| name.trace('t')`                     | 6        |

**Note on trace-entry granularity:** the issue reports 3 entries for
`name.trace('t')` against a 3-name patient, which implies per-element
collector semantics rather than per-row. Before writing assertions, tasks
include a small calibration step: run `name.trace('t')` alone and record
what `collector.getEntries().size()` actually returns. If it's 1 (per-row),
adjust the expected values accordingly — the bug ratios (2×, 3×, 4×)
remain the same, only the base count shifts. This is documented as
Task 1.

## Risks / Trade-offs

**Risk:** The test entry-count semantics may not match the issue author's
numbers (per-row vs per-element) → Mitigation: calibration step as Task 1
before writing assertions. If the base counts differ, the matrix is
re-derived by multiplying issue ratios against the observed base count.

**Risk:** CI goes red while the tests wait for the fix → Mitigation: D4's
`@Tag("known-failing")` approach, which keeps the tests present and
discoverable but excluded from default test runs. The tag is a clear marker
that the failure is expected.

**Risk:** A fix that goes further than option 1 (e.g. refactors
`TraceExpression`) could change entry-count semantics subtly → Mitigation:
the regression guard rows (currently passing) serve as a bidirectional
check. Any fix must keep those rows green AND turn the failing rows green.

**Risk:** The reproduction fixture drifts from the exact JSON in the issue →
Mitigation: embed the JSON as a text block in the test class and comment
that it must not be altered without updating #2594 reference.

## Migration Plan

Not applicable — test-only change, no deployment surface.

## Open Questions

- Q: Does Pathling's CI configuration support Surefire tag exclusion out
  of the box?
  A: Tasks include verifying this. If not, fall back to splitting into
  passing/failing methods with `@Tag` on the failing one.

- Q: Should the known-failing tag name be `known-failing`, `bug-2594`, or
  something else?
  A: Deferred to the task-level; no impact on the design.
