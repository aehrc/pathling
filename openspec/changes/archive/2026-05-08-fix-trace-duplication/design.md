## Context

Issue #2594 documents that a single source-level FHIRPath `trace()` call
produces 2× or more `TraceCollector` entries when its result column is
consumed by `count`, `exists`, `empty`, `last`, `combine`, or the `|`
union operator. The reproduction change (archived
2026-04-24-reproduce-trace-duplication) added a parametrised test
matrix and a spec requirement; six matrix rows are tagged
`known-failing` and exclude themselves from the default Surefire run.

Investigation, including hands-on spikes against Spark 4.0.1, has
established the following:

1. **Why CSE doesn't fix it.** `TraceExpression` is `Nondeterministic`
   (deliberately, to prevent Catalyst from eliding side effects).
   Catalyst's common-subexpression elimination excludes
   `Nondeterministic` expressions outright. Even if it did not,
   `EquivalentExpressions.childrenToRecurse` is conservative around
   `CaseWhen`: a subexpression that appears once in
   `alwaysEvaluatedInputs` (the predicate) and once in a single
   conditional branch is not registered as a common subexpression.

2. **Why Catalyst's `With` expression is NOT a viable primitive
   for Pathling.** `org.apache.spark.sql.catalyst.expressions.With`
   is a let-binding that the `RewriteWithExpression` optimiser rule
   lowers into a `Project` operator. That makes it a logical-plan
   construct, not a pure column expression. Pathling's contract for
   FHIRPath compilation is that the resulting `Column` must be
   embeddable in any relational context — `select`, `filter`,
   `join`, `groupBy.agg`, `Window.over`. `With` violates this
   contract: its rewrite rule has only partial aggregate support and
   no window support, and the rule asserts hard against certain
   aggregate-with-let combinations at construction time. A FHIRPath
   `Column` produced via `With` may execute in some contexts and
   fail in others, with no reliable way to predict which.

    A spike further confirmed that wrapping `TraceExpression` in
    `With` at construction time also fails for the bug class itself,
    because the rule's special-case handling for
    `ConditionalExpression` _inlines_ nested `With`s (preserving
    short-circuit evaluation semantics). The `With` must directly
    wrap the conditional, which means even a localised use is brittle
    under arbitrary downstream usage. `With` is therefore rejected
    as the implementation primitive.

3. **Why the lambda-let pattern is the right primitive.** Spark's
   higher-order array functions (`transform`, `aggregate`, `filter`)
   support a let-binding idiom that produces a pure `Column`
   expression with no logical-plan dependency:

    ```
    let(c, x -> body(x))
      ≡ element_at(transform(array(c), x -> body(x)), 1)
      ≡ aggregate(array(c), <typed_null>, (acc, x) -> body(x))
    ```

    `array(c)` evaluates `c` exactly once at codegen time. The
    higher-order function then invokes the body lambda with `x` bound
    to the materialised value. The lambda parameter is a positional
    stack reference; multiple references to `x` in the body do not
    re-evaluate `c`. The resulting expression is a regular Spark
    `Column` and embeds in every relational context.

4. **Spike outcomes (recorded only here; not in committed code).**
    - Buggy `when(c.isNull, 0).otherwise(size(c))` against a
      `TraceExpression` operand: 4 fires for 3 rows (2 non-null × 2).
    - Lambda-let via `transform`: 2 fires (1 per non-null row).
    - Lambda-let via `aggregate`: 2 fires.
    - Builtin `coalesce(size(c), lit(0))`: 2 fires.
    - Per-row dedup is per-row, not per-query.
    - Inside `Window.over`: works without errors; trace fires equal
      Spark's window-engine evaluation count of the column (which
      may exceed 1 per row for ordering+select combined). The let
      pattern halves the count compared to the bug pattern in this
      context, as expected.
    - Inside SQL aggregates (`sum`, `count`): Spark refuses with
      `AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`. This
      is a Spark constraint on `Nondeterministic` expressions
      irrespective of whether `let` is used; the same error occurs
      with `sum(coalesce(size(traceCol), 0))`. Out of scope for the
      bug fix; documented as a known limitation (D8).

## Goals / Non-Goals

**Goals:**

- All 10 `TraceEntryCountTest` rows pass without the `known-failing`
  tag. The duplicating-operations method runs in the default Surefire
  configuration.
- Forward-looking guard: a future
  `ColumnRepresentation` method that re-introduces a multi-reference
  `when/otherwise` pattern fails CI, either via the new unit-level
  test layer or the Checkstyle rule, ideally both.
- Per-row values produced by every rewritten method are identical to
  the current implementation across every existing test.
- The result of every rewritten method is a pure Spark `Column`
  expression that composes in arbitrary relational contexts.
- Public Spark functions API only — no Catalyst-internal classes
  imported into Pathling production code.

**Non-Goals:**

- Modifying `TraceExpression`, `TraceProjectionExpression`, or any
  collector implementation.
- Changing the determinism contract of trace.
- Modifying the user-facing FHIRPath grammar, function registry, or
  any language binding.
- Generalising the `let` helper beyond the fhirpath module.
- Auditing every Spark column construction in the wider codebase for
  the same pattern. The Checkstyle rule is scoped to
  `ColumnRepresentation` and can be extended later if needed.
- Removing the Spark `Nondeterministic`-in-aggregate restriction
  (a documentation-only follow-up; see D8).

## Decisions

### D1. Lambda-let pattern via Spark higher-order functions

The `let` helper is implemented over `array(value)` + `transform` (or
`aggregate`) rather than over Catalyst's `With` expression. The
result is a pure column expression that embeds in any relational
context, including inside `Window.over` and `select`/`filter`/`join`.
SQL-aggregate use is gated by Spark's `Nondeterministic` constraint
(D8), independent of the let mechanism.

**Alternatives considered and rejected:**

- _Catalyst `With` expression_ — rejected because `With` is rewritten
  into a `Project` operator at the logical-plan level, breaking
  Pathling's pure-column contract for FHIRPath compilation. Window
  context unsupported, aggregate context partial. A spike further
  showed that wrapping `TraceExpression` in `With` at construction
  fails for the bug class itself due to the rule's
  `ConditionalExpression` inlining special case.
- _Drop `Nondeterministic` from `TraceExpression`_ (issue option 3) —
  rejected. The `fhirpath-trace` capability already requires trace
  nondeterminism (scenario "duplicate trace calls both execute").
  Removing it would invalidate that requirement and gamble on Spark
  not introducing future cross-row trace caching.
- _Memoise `TraceExpression` evaluation_ (issue option 2) — rejected.
  Adds per-row cache state, requires defining "row boundary" in
  Spark's iterator model, and produces false positives where two
  array elements with byte-identical contents collapse to one
  collector entry.
- _Dedupe in `ListTraceCollector`_ (issue option 4) — rejected. Wrong
  place; hides plan behaviour and can't tell apart two genuinely
  identical values from a doubled fire.
- _Custom Pathling Catalyst `LetExpression`_ — possible (would be a
  hand-rolled `With` that never goes through plan rewriting), but
  introduces a new custom Catalyst expression with the usual
  maintenance burden. Lambda-let achieves the same semantic with
  stable public Spark functions and zero custom expression code.
  Reserved as a future optimisation if the small array-allocation
  overhead ever matters in a hot path.

### D2. The `let` helper

API:

```java
public final class ColumnHelpers {
  /**
   * Evaluates {@code value} exactly once per row and binds it to the
   * lambda parameter. Multiple references inside {@code body} read
   * from a single materialised value — they do not re-evaluate the
   * operand. The returned expression is a pure Spark Column with no
   * logical-plan dependency, so it composes in any relational
   * context.
   */
  @Nonnull
  public static Column let(
      @Nonnull final Column value,
      @Nonnull final UnaryOperator<Column> body) {
    return functions.element_at(
        functions.transform(functions.array(value), body::apply),
        1);
  }
}
```

The `transform` variant is preferred over `aggregate` for the
generic helper because it does not require the caller to supply a
typed null initial value (the result type is inferred from `body`).
At call sites where the body's return type is locally known, an
inline `aggregate(array(c), <typed_null>, (acc, x) -> body(x))` is
acceptable and saves one array allocation; the helper version is
the default.

**Location.** New class
`fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/ColumnHelpers.java`.
Same package as `ColumnRepresentation`, so call sites read naturally
(`let(c, x -> ...)`).

**Constraint.** The class-level Javadoc notes that the returned
expression is `Nondeterministic` if and only if `value` is
`Nondeterministic` — i.e. the helper is transparent to the
side-effect contract of its operand. Spark's
`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION` rule therefore
applies to `let(traceCol, …)` exactly as it would to `traceCol`
directly (D8).

### D3. Per-method rewrite table

For every method, prefer Spark builtins that reference the operand
once. Use `let` only where both branches of a conditional genuinely
need the operand. References-to-`c` count is `current → new`.

| Method            | Branch | Current                                                                     | Rewrite                                                                | Refs  |
| ----------------- | ------ | --------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ----- |
| `count()`         | array  | `when(c.isNull(), 0).otherwise(size(c))`                                    | `coalesce(size(c), lit(0))`                                            | 2 → 1 |
| `count()`         | scalar | (already single-ref)                                                        | unchanged                                                              | 1     |
| `isEmpty()`       | array  | `when(c.isNotNull(), size(c)===0).otherwise(true)`                          | `coalesce(size(c).equalTo(0), lit(true))`                              | 2 → 1 |
| `isEmpty()`       | scalar | `Column::isNull`                                                            | unchanged                                                              | 1     |
| `last()`          | array  | `when(c.isNull() \|\| size(c)===0, null).otherwise(element_at(c, size(c)))` | `try_element_at(c, -1)`                                                | 3 → 1 |
| `normaliseNull()` | array  | `when(c.isNull() \|\| size(c)===0, null).otherwise(c)`                      | `nullif(c, array())`                                                   | 2 → 1 |
| `aggregate()`     | array  | `when(c.isNull(), zero).otherwise(functions.aggregate(c, zero, agg))`       | `coalesce(functions.aggregate(c, lit(zero), agg), lit(zero))`          | 2 → 1 |
| `aggregate()`     | scalar | `when(c.isNull(), zero).otherwise(c)`                                       | `coalesce(c, lit(zero))`                                               | 2 → 1 |
| `plural()`        | array  | `when(a.isNotNull(), a).otherwise(array())`                                 | `coalesce(a, array())`                                                 | 2 → 1 |
| `plural()`        | scalar | `when(c.isNotNull(), array(c)).otherwise(array())`                          | `filter(array(c), x -> x.isNotNull())`                                 | 2 → 1 |
| `singular()`      | array  | `when(c.isNull() \|\| size(c)<=1, getAt(c,0)).otherwise(raise_error)`       | `let(c, x -> when(size(x).gt(1), raise_error).otherwise(getAt(x, 0)))` | 3 → 1 |
| `filter()`        | scalar | `when(c.isNotNull(), when(lambda.apply(c), c))`                             | `let(c, x -> when(x.isNotNull().and(lambda.apply(x)), x))`             | 3 → 1 |
| `toArray()`       | scalar | `when(c.isNotNull(), array(c))`                                             | `let(c, x -> when(x.isNotNull(), array(x)))`                           | 2 → 1 |
| `transform(λ)`    | scalar | `when(c.isNotNull(), lambda.apply(c))`                                      | `let(c, x -> when(x.isNotNull(), lambda.apply(x)))`                    | 2 → 1 |

Rewrite categories:

- **Pure Spark builtin (no `let`):** `count` array, `isEmpty` array,
  `last`, `normaliseNull`, `aggregate` (both branches), `plural`
  (both branches). Single-reference rewrites using `coalesce`,
  `element_at(c, -1)`, `nullif`, `filter(array(c), …)`.
- **`let` helper:** `singular`, `filter` scalar, `toArray` scalar,
  `transform` scalar. Conditionals where both branches need `c`.
- **Already single-ref (unchanged, regression-guarded by Layer B):**
  `first`, `orElse`, `ensureSingular`, `removeNulls`, `exists`,
  `count` scalar, `isEmpty` scalar.

The pattern: **use Spark builtins where they reference the operand
once; reach for `let` only when both branches need the operand.**

### D4. Test layering

Two layers, each with a distinct purpose. Both are mandatory.

**Layer A — extend `TraceFunctionTest$TraceEntryCountTest`.** Drop
`@Tag("known-failing")` from the duplicating-operations method.
Augment the FHIRPath matrix with rows that exercise additional
surface likely to compile to multi-reference patterns:

- `name.trace('t').single()` (singular)
- `name.trace('t').iif(name.given.exists(), 'a', 'b')` (CaseWhen-shaped helper)
- `name.trace('t').given.count() > 0` (count + comparison)

This layer is the user-visible regression guard. New rows are
expected to pass once D3 is applied.

**Layer B — new `ColumnRepresentationTraceTest`.** Located at
`fhirpath/src/test/java/au/csiro/pathling/fhirpath/column/ColumnRepresentationTraceTest.java`.
Constructs a `TraceExpression` operand directly, wraps it in a
`DefaultRepresentation`, calls each public method that operates on
its operand, and asserts `collector.count == expected_per_row` for
both single-row and multi-row inputs. One row per offending method
plus one sanity row per single-reference method. Forward-looking
guard against any future helper that re-introduces a multi-reference
shape.

**Alternative considered:** Layer A alone. Rejected because helpers
like `aggregate` (scalar branch), `normaliseNull`, `toArray`,
and `defaultIfNull` are not directly accessible from FHIRPath but
still implement the contract. Without Layer B, a regression in
those helpers would only surface when some user-visible FHIRPath
function happened to route through them.

### D5. Checkstyle rule — identifier-repetition regex

Goal: any new conditional Spark column construction in
`ColumnRepresentation.java` (and similar SQL-builder files) that
references the same identifier in both the predicate and a value
branch must be flagged. The rule does not parse Spark expression
trees; it works at the Java token level using regex backreferences.

**Mechanism.** Two `RegexpMultiline` Checkstyle modules, scoped via
`<files>`. The first catches the
`when(P uses x, V uses x)` shape; the second catches the
`when(P uses x).otherwise(V uses x)` shape:

```xml
<module name="RegexpMultiline">
  <property name="format"
    value="\bwhen\s*\(\s*[^()]*?\b(\w+)\b[^()]*?,\s*[^()]*?\b\1\b"/>
  <property name="message"
    value="Possible repeated SQL evaluation: same identifier in
           when() predicate and value. Wrap in let() or restructure.
           See issue #2594."/>
  <property name="fileExtensions" value="java"/>
</module>
<module name="RegexpMultiline">
  <property name="format"
    value="\bwhen\s*\(\s*[^()]*?\b(\w+)\b[^)]*\)\s*\.\s*otherwise\s*\([^)]*?\b\1\b"/>
  <property name="message"
    value="Possible repeated SQL evaluation: same identifier in when()
           and otherwise(). Wrap in let() or restructure.
           See issue #2594."/>
  <property name="fileExtensions" value="java"/>
</module>
```

Walked manually against `ColumnRepresentation.java`:

- All 12 known-buggy patterns are flagged.
- All 5 legitimate single-ref `when` calls are NOT flagged
  (`count` scalar, `isEmpty` scalar via method ref, `exists`
  array, `exists` scalar, `ensureSingular`).

**False positives.** Inside a `let(c, x -> body)`, `x` is a
materialised binding, so multi-ref to `x` is safe — but the regex
sees `x` twice and flags it. We expect ~4 such suppressions in
`ColumnRepresentation` post-fix:

```java
return vectorize(
    /* array */ a -> coalesce(a, array()),
    /* scalar */ c -> let(c,
        // SUPPRESS RepeatedSqlEvaluation: inside let body
        x -> when(x.isNotNull(), array(x))));
```

Suppression markers are honoured via the standard
`SuppressWithNearbyCommentFilter`. Each suppression doubles as
documentation: it asserts that the author confirmed `x` is the
materialised binding rather than a re-evaluation.

**Alternatives considered:**

- _Custom Checkstyle module_ that walks the AST and detects shared
  identifiers in `when(...).otherwise(...)`: more accurate (zero
  false positives), but ~150 lines of Java + tests +
  `checkstyle-core` dependency. Disproportionate for ~4 sites.
- _"Forbid all `when(`" regex_: simpler but flags every legitimate
  single-reference `when` call (~5 of them). Higher suppression
  noise without commensurate value.

**Files in scope:**

- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/ColumnRepresentation.java`
- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`
- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/EmptyRepresentation.java`
- Other `*Representation*.java` files in the same package, by glob.

The rule is configured in the existing `checkstyle.xml`.

### D6. Spark `size(null)` and `element_at` null-safety

Several rewrites depend on Spark's null-semantics:

- `size(null) === null` (with `spark.sql.legacy.sizeOfNull = false`,
  the Spark 3.0+ default).
- `element_at(null_array, n) === null` for any `n`.
- `element_at(arr, -1)` returns the last element of `arr`, or null
  when `arr` is empty/null.
- `aggregate(null, …)` returns null.
- `coalesce(null, x)` returns `x`.
- `nullif(null, _) === null` and `nullif(empty_array, empty_array) === null`.

Pathling does not currently set `spark.sql.legacy.sizeOfNull`. The
default has been `false` since Spark 3.0 (5+ years).

**Decision:** task-level addition of an assertion at Pathling Spark
configuration time that `spark.sql.legacy.sizeOfNull` is `false`.
Documented as required for FHIRPath cardinality semantics. Any
deployment that flips it to `true` would silently change `count()`
and `isEmpty()` behaviour on null inputs.

### D7. ANSI mode and array element access

Pathling runs Spark 4.0.2. In Spark 4.0+, `spark.sql.ansi.enabled`
defaults to `true`; Pathling does not override it. ANSI is therefore
the live execution mode. Several rewrites in D3 must use the
explicitly null-safe array-access functions to avoid throwing on
edge inputs (empty arrays, null arrays):

- **`last()`** uses `try_element_at(c, -1)`, not `element_at(c, -1)`.
  Under ANSI, `element_at` on an out-of-range index (including any
  index against an empty array) raises `INVALID_ARRAY_INDEX`.
  `try_element_at` is the ANSI-safe variant that returns null
  instead. This matches the original `last()` semantics
  (null for null/empty input) on a single reference to `c`.
- **`singular()`** and **`first()`** continue to use the existing
  `getAt(c, idx)` helper, which wraps `functions.get(arr, idx)`.
  `functions.get` is Spark's explicitly null-safe 0-indexed
  array-access function (added in Spark 3.4 to provide ANSI-safe
  access). It returns null for any out-of-range index, including
  negatives and any access against null/empty arrays. No change
  needed for these methods beyond the let-wrapping.
- **`size(c)`** is unaffected by ANSI mode. Its return on a null
  array depends on `spark.sql.legacy.sizeOfNull`, which Pathling
  does not set; the default is `false` in Spark 3.0+, so
  `size(null) = null`. This is what `coalesce(size(c), lit(0))`
  in the `count()` rewrite relies on.
- **`coalesce`, `nullif`, `transform`, `aggregate`, `filter`,
  `array`, `lit`** are not affected by ANSI mode for the inputs we
  use.

A code comment in each rewrite that depends on ANSI semantics
(`last()`, possibly `count()` if we wanted to be defensive) names
the dependency explicitly.

**Risk if a deployment overrides ANSI off.** No issue — the
ANSI-safe variants we use (`try_element_at`, `functions.get`,
`coalesce(size(...), 0)`) behave identically with ANSI on or off.
The choice of variants is forward-compatible with both modes.

### D8. `trace()` inside SQL aggregates is a Spark constraint

Spark's analyzer rejects `Nondeterministic` expressions inside SQL
aggregate functions with
`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`. This applies
to any FHIRPath expression containing a `trace()`, regardless of how
the column is constructed (with or without `let`, with or without
the rewrites in this change). The same error reproduces with
`sum(traceCol.isNull())` or `sum(coalesce(size(traceCol), 0))`.

**Decision:** treat as a documentation issue, not a code change.
File a follow-up GitHub issue (referenced from this change's
`tasks.md`) that adds a paragraph to the FHIRPath `trace()`
documentation page noting:

- `trace()` produces a `Nondeterministic` expression by design.
- Spark forbids `Nondeterministic` expressions inside SQL aggregate
  functions (`sum`, `count`, `avg`, …).
- A traced FHIRPath expression cannot be aggregated in this way; if
  aggregation is required, use a non-traced expression and add the
  `trace()` upstream of the aggregation boundary.

This change does not modify any production code path related to
this constraint. The follow-up issue is the action item; the design
captures the rationale for not addressing it here.

**Follow-up issue:** #2607 — "Document `trace()` incompatibility with
SQL aggregate functions". The issue body references this design doc's D8
section, the Spark error class
`AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION`, and the
user-facing workaround (move `trace()` upstream of the aggregation
boundary).

## Risks / Trade-offs

- **[Lambda-let allocation overhead]** Each call to `let` constructs
  a single-element array and invokes a higher-order function over
  it. Codegen may or may not elide this in whole-stage compilation
  on Spark 4.0.x. → Mitigation: accept the overhead. Benchmarking
  is not a goal of this change. If a hot path ever shows the
  overhead matters, swap the helper's implementation to a custom
  Catalyst `LetExpression` (the public helper API stays stable).

- **[`size(null)` config flip]** A deployment that sets
  `spark.sql.legacy.sizeOfNull = true` silently breaks `count()` and
  `isEmpty()` on null inputs. → Mitigation: D6's startup assertion.

- **[ANSI array-access semantics]** Pathling runs Spark 4.0.2 with
  ANSI enabled by default. → Mitigation: D7's choice of
  `try_element_at(c, -1)` for `last()`, `functions.get` (via the
  existing `getAt`) for `singular`/`first`, and `coalesce`-based
  rewrites for everything else. All rewrites are ANSI-safe. No
  deferred switch needed.

- **[Checkstyle false positives in `let` bodies]** ~4 expected
  suppressions in `ColumnRepresentation` post-fix. → Mitigation:
  the `// SUPPRESS RepeatedSqlEvaluation: inside let body` comment
  documents the intent and is reviewed at the same time as the code.

- **[`let` over `Nondeterministic` operand and SQL aggregates]**
  `let(traceCol, …)` is itself `Nondeterministic` (the helper is
  transparent to the operand's contract), so it inherits the Spark
  aggregate restriction. → Mitigation: D8's documentation issue.
  No production code change.

- **[Test layer overlap]** Layer A and Layer B both check trace fire
  counts for some methods. → Mitigation: accepted. The redundancy
  provides defence in depth; cost is small.

- **[Evaluator differences]** `TraceFunctionTest` uses
  `SingleInstanceEvaluator`. Layer B will run direct
  `df.select(...)` calls. → Mitigation: Layer B asserts ratios where
  appropriate (single-fire per row, halved fire-count compared to
  bug pattern), so absolute calibration differences don't break the
  detection.

## Migration Plan

Not applicable — fix-only change, no public API or data model
changes. Behaviour difference is observable only through trace
collector entry counts (now correct) and possibly through marginal
performance improvements for expensive deterministic operands that
were previously recomputed inside CaseWhen branches.

Rollback: revert this change. The `known-failing` tag, the
rewrites, the `let` helper, and the Checkstyle rule revert as a
single unit. The follow-up documentation issue (D8) is independent
and stays open across rollback.

## Open Questions

- **Q1.** D6: assertion vs. defensive coalesce for
  `spark.sql.legacy.sizeOfNull`? Recommendation: add an assertion at
  Spark startup (loud) plus keep the rewrites assuming the default.
  Defer the final call to task-level review.
- **Q2.** Layer A matrix expansion: which additional FHIRPath
  expressions are most worth covering? Recommendation: `single`,
  `iif`, and one count-comparison. Final list confirmed during
  task-level implementation.
- **Q3.** Should the `aggregate` variant of the lambda-let pattern
  be exposed as a second helper (`letAgg(value, init, body)`) for
  call sites that want to skip the `element_at` step? Recommendation:
  no, until a benchmark shows it matters. Inline `aggregate(...)`
  at the rare site that needs it.
