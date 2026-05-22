## Why

Issue #2594 documents a concrete bug: `trace()` entries captured by
`PathlingContext.evaluateFhirPath()` are emitted more than once when the traced
column is consumed by any FHIRPath operation that compiles into a
`when(cond(c), …).otherwise(expr(c))` pattern in `ColumnRepresentation`
(`count`, `exists`, `empty`, `first`, `last`, `|`, `combine`, …). The root
cause is understood: `TraceExpression` is `Nondeterministic`, so Catalyst's
common-subexpression elimination cannot dedupe it, and Spark's CSE is also
conservative around `CaseWhen` branches even when an expression is
deterministic (verified against Spark 4.0.2).

A fix is coming in a separate change. Before it lands we want the bug pinned
down with a regression test suite so that (a) the eventual fix has an
unambiguous acceptance oracle, (b) no future refactor silently re-introduces
the duplication, and (c) the failure is visible in CI rather than living only
in a GitHub issue.

This change is intentionally scoped to **tests only** — no production code
changes, no fix attempt.

## What Changes

- Add a new `@Nested` test class in
  `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/provider/TraceFunctionTest.java`
  (e.g. `TraceEntryCountTest`) that asserts the number of `ListTraceCollector`
  entries produced for each expression in the reproduction matrix from #2594.
- Use a single Patient fixture with three `name` entries (matching the issue)
  so expected counts are distinguishable from any doubling/tripling.
- Parametrise the test so the 11-row matrix renders as 11 distinct JUnit
  cases rather than one big assertion block.
- The currently-passing rows of the matrix (the ones that already produce the
  correct entry count) act as a regression guard so any fix does not break
  them.
- The currently-failing rows of the matrix are kept enabled (NOT `@Disabled`)
  and tagged so CI users can distinguish them from genuine regressions.
  The fix-change will retire the tag when the assertions pass.
- Add a new requirement to the `fhirpath-trace` capability stating that the
  number of collector entries produced by a single source-level `trace()`
  call SHALL equal the number of logical invocations of that trace, regardless
  of the shape of downstream operations that consume the traced column.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `fhirpath-trace`: add a "trace entry count fidelity" requirement. The
  existing nondeterminism requirement states that separate `trace()` calls
  must each execute; the new requirement states that a single `trace()` call
  must not be duplicated by downstream compilation patterns. The matrix of
  expressions from #2594 becomes the scenario set.

## Impact

- Test code only: new tests added under `fhirpath/src/test/java/...`.
- No changes to production code or public API.
- CI: the failing rows of the matrix will produce test failures until the
  separate fix change lands. This is intentional — the red signal is the
  point. The tests will be tagged so they can be excluded from default
  runs if necessary during the interim (see `design.md` for the exact
  mechanism).
- No dependency, configuration, or build changes.
