## Why

Issue #2594 documents a concrete defect: a single source-level `trace()`
call produces multiple `TraceCollector` entries — typically 2× or 3× the
expected count — when the traced column is consumed by FHIRPath
operations whose Spark column form references the operand more than
once (`count`, `exists`, `empty`, `first`, `last`, `combine`, `|`).

The reproduction change (archived 2026-04-24-reproduce-trace-duplication)
pinned the bug down with a 10-row test matrix tagged `known-failing`,
and added a spec requirement to the `fhirpath-trace` capability stating
that collector entries must match the number of logical trace
invocations regardless of downstream plan shape.

This change implements the fix. It must (a) turn the known-failing rows
green without breaking the passing rows, and (b) prevent the same bug
class from reappearing in future code.

## What Changes

- Introduce a `ColumnHelpers.let(Column value, UnaryOperator<Column> body)`
  helper implementing the lambda-let pattern over Spark's higher-order
  array functions: `array(value)` materialises the operand once, then
  `transform` (or `aggregate`) invokes the body lambda with the
  materialised value bound to its parameter. The helper produces a
  pure Spark `Column` expression with no logical-plan dependencies,
  so it composes correctly inside any relational context (select,
  filter, join, window).
- Rewrite the `ColumnRepresentation` methods that currently compile
  into multi-reference Spark patterns:
  `count` (array branch), `isEmpty` (array branch), `last`,
  `normaliseNull`, `aggregate` (both branches), `plural` (both
  branches), `singular`, `filter` (scalar branch), `toArray` (scalar
  branch), and `transform` (scalar branch).
  Where a Spark builtin lets us reference the operand exactly once
  (`coalesce(size(c), 0)`, `try_element_at(c, -1)`,
  `nullif(c, array())`, `coalesce(c, zero)`,
  `filter(array(c), x -> x.isNotNull())`), prefer the builtin. Where
  both branches of a conditional genuinely need the operand, use
  `let`. All chosen builtins are ANSI-safe (Pathling runs Spark
  4.0.2 with ANSI mode enabled by default).
- Extend the trace test suite at two layers:
    - **Layer A** — extend `TraceFunctionTest$TraceEntryCountTest`'s
      FHIRPath matrix to cover additional surface (`single()`, `iif()`,
      additional combinations) so the user-visible regression coverage
      grows with the fix.
    - **Layer B** — add a new unit test class
      `ColumnRepresentationTraceTest` with one row per
      `ColumnRepresentation` method that operates on its operand,
      asserting single-fire trace semantics for each. This catches
      future helpers that re-introduce the bug pattern in code paths
      the FHIRPath surface doesn't directly expose.
- Remove the `known-failing` tag from `TraceEntryCountTest`'s
  duplicating-operations rows once they pass, so all entry-count
  scenarios run by default.
- Add a Checkstyle rule scoped to `ColumnRepresentation.java` (and
  similar SQL-builder files in the same package). The rule uses
  identifier-repetition regexes to flag any
  `when(...x...).otherwise(...x...)` or `when(...x..., ...x...)`
  pattern — i.e. the same identifier appears in both the predicate
  and a value branch. Legitimate uses inside `let` bodies (where the
  identifier is a materialised binding) carry a per-line
  `// SUPPRESS RepeatedSqlEvaluation: inside let body` comment.
- File a follow-up GitHub issue documenting that `trace()` cannot
  be used inside SQL aggregates (`sum`, `count`, `avg`, …). This is
  a pre-existing Spark constraint
  (`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`) on
  `Nondeterministic` expressions, not introduced by this fix; the
  issue records the limitation in user-facing documentation.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `fhirpath-trace`:
    - The "trace entry count matches logical invocations" requirement
      (added by the reproduction change) gains additional scenarios
      covering FHIRPath surface introduced by Layer A: `single()`,
      `iif()`, and `count() > N`. The requirement statement itself is
      unchanged; only its scenario set grows.
    - A new requirement documents the pre-existing Spark constraint
      that `trace()` (a `Nondeterministic` expression) cannot appear
      inside SQL aggregate functions (`sum`, `count`, `avg`, …).
      This makes the limitation visible in the spec rather than
      living only in implementation knowledge or documentation.

## Impact

- **Production code:** `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/ColumnRepresentation.java`
  is rewritten across roughly ten branches (a mix of array and scalar
  lambdas inside `vectorize` calls). A new helper class
  `ColumnHelpers` in the same package holds the `let` primitive.
- **Tests:** `TraceFunctionTest` matrix grows, the `known-failing` tag
  is dropped from its previously-failing rows, and a new
  `ColumnRepresentationTraceTest` is added in
  `fhirpath/src/test/java/au/csiro/pathling/fhirpath/column/`.
- **Build:** Checkstyle configuration gains two new
  `RegexpMultiline` modules scoped via `<files>` to
  `ColumnRepresentation.java` and similarly-named SQL-builder files.
  No new build dependencies.
- **Spark API surface:** the change uses only public Spark
  `org.apache.spark.sql.functions` calls (`transform`, `aggregate`,
  `array`, `try_element_at`, `coalesce`, `nullif`, `size`,
  `functions.get`). No Catalyst-internal types are introduced. The
  lambda-let pattern is a standard Spark idiom for emulating
  let-bindings without modifying the relational plan. All chosen
  array-access functions are ANSI-safe (return null on out-of-range
  rather than throw), matching Spark 4.0's default ANSI mode.
- **Relational composability preserved:** because the rewritten methods
  produce pure Spark `Column` expressions, their results compose
  correctly inside `select`, `filter`, `join`, and `window` contexts.
  Pre-existing Spark restrictions on `Nondeterministic` expressions
  inside SQL aggregate functions (`sum`, `count`, …) still apply to
  any FHIRPath expression that contains a `trace()`; this is a Spark
  constraint, not a regression introduced by the fix, and is captured
  in the documentation issue called out above.
- **Behaviour preserved:** all rewritten methods produce identical
  per-row values to their current implementations across the existing
  test suite. The change is observable only through the trace fire
  counts and (incidentally) through marginal performance improvements
  for any expensive deterministic operand previously hidden from CSE
  by the conditional-branch pattern.
- **Public API:** unchanged. The `let` helper is internal to the
  fhirpath module; no language-binding (Python, R) or library-API
  surface is affected.
