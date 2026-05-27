## Context

Pathling's FHIRPath engine already implements the `|` union operator via
`UnionOperator` (extending `SameTypeBinaryOperator`). That implementation
handles type reconciliation (e.g. Integer → Decimal promotion), Decimal
precision normalization to DECIMAL(32,6), empty-operand semantics, and
comparator-aware deduplication (default SQL equality for simple types;
element-wise equality for Quantity, Coding, and temporal types via
`SqlFunctions.arrayDistinctWithEquality` / `arrayUnionWithEquality`).

The FHIRPath specification requires two equivalent function-form entry
points in §6.5 Combining:

- `union(other : collection) : collection` — declared synonymous with `|`.
- `combine(other : collection) : collection` — union without deduplication.

Neither function currently exists. Users writing the function form see a
"function not found" error even though `|` works. `combine()` has no
equivalent operator at all.

### The iteration-context problem

The spec explicitly states, via its worked example:

> `name.select(use.union(given))` is the same as `name.select(use | given)`,
> noting that the union function does not introduce an iteration context

For this equivalence to hold inside an iterating function such as
`select`, the `given` argument must resolve against the current name
element, not against the root `%context`.

Pathling's current architecture cannot deliver that equivalence if
`union()` is implemented as a regular method-defined function:

1. The parser turns `use.union(given)` into
   `Composite([Traversal(use), EvalFunction(union, [given])])`.
2. `Composite.apply(n, ec)` reduces left-to-right: the first element
   produces `n.use`, which then becomes the input to the second
   element; the original `n` is discarded from the chain.
3. When `EvalFunction(union, [given])` invokes the function,
   `FunctionParameterResolver` evaluates the `given` argument against
   `evaluationContext.getInputContext()`. That field is fixed at
   construction time (`FhirEvaluationContext.inputContext`) and returns
   the root resource — **not** the current iteration item `n`.

In contrast, the `|` operator takes the chained form
`EvalOperator(leftPath=use, rightPath=given, op=UnionOperator)` and its
`invokeWithPaths` evaluates _both_ operand paths against the current
input `n`, so `given` resolves correctly.

This gap would only surface for arguments that are relative paths
evaluated inside an iteration context. It has not been exercised before
because no existing function takes a `Collection` non-lambda argument —
`union()`/`combine()` are the first.

Fixing the resolver architecture to propagate an "outer focus" through
`Composite.apply` is a significant refactor with broad risk. Fortunately
the spec itself describes `union()` as _synonymous_ with `|`, which
invites a cheaper solution: rewrite `x.union(y)` and `x.combine(y)` to
operator-form ASTs at parse time.

## Goals / Non-Goals

**Goals:**

- Implement `union()` and `combine()` as FHIRPath surface features that
  are strictly observationally equivalent to the `|` operator (for
  `union`) or to a structurally identical operator (for `combine`).
- Preserve exact semantic equivalence between `x.union(y)` and `x | y`
  for every input shape, including arguments that are relative paths
  inside iteration contexts.
- Share code between the `|` operator, `union()`, and `combine()` so
  that type reconciliation, Decimal normalization, and comparator
  handling live in exactly one place.
- Match the type-reconciliation policy of `|` for both functions,
  including rejecting incompatible polymorphic types (e.g.
  Integer ∪ Boolean) consistently.
- Provide DSL test coverage that mirrors the existing `|` coverage and
  adds `combine()`-specific duplicate-preservation cases, with
  pin-down tests for the iteration-context equivalence.

**Non-Goals:**

- Changing the behaviour of `|` for any input.
- Refactoring `Composite`/`EvalFunction`/`FunctionParameterResolver` to
  propagate an "outer focus" through chained expressions. That is a
  broader architectural change that would benefit future
  `Collection`-argument functions but is out of scope for this issue.
- Supporting polymorphic unions across incompatible types (this remains
  a known limitation tracked in the YAML config).
- Lifting the indefinite calendar duration limitation in union
  deduplication.
- Exposing `union()` / `combine()` as standalone (subject-less) function
  calls. The spec defines them only as method invocations on an input
  collection, so the parser desugaring will only recognise the
  `x.union(y)` / `x.combine(y)` shape.
- Adding language bindings (Python / R) beyond what the FHIRPath engine
  surface already exposes automatically.

## Decisions

### Decision 1: Extract a shared `CombiningLogic` helper

Both the `|` operator and the new `combine` operator need the same
pipeline for the merge step:

```
get array column  →  normalise Decimal  →
  (dedupe | concatenate)  →  wrap in the reconciled Collection
```

Options considered:

| Option                                                            | Verdict                                                                                  |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Extract a shared helper (`CombiningLogic`) used by both operators | **Chosen**                                                                               |
| Duplicate the array plumbing in each operator                     | Rejected: two places to fix decimal / comparator bugs                                    |
| Call `UnionOperator` directly from `CombineOperator`              | Rejected: `union` dedupes, `combine` does not — the entry points are genuinely different |

The helper is placed in the operator package (peer to `UnionOperator`
and the new `CombineOperator`) and exposes small composable primitives:

- `prepareArray(Collection)` — extract the array column, normalising
  Decimal precision.
- `dedupeArray(Column, ColumnEquality)` — deduplicate using either
  Spark's `array_distinct` or `SqlFunctions.arrayDistinctWithEquality`
  depending on the comparator.
- `unionArrays(Column, Column, ColumnEquality)` — merge + dedupe two
  arrays.
- `combineArrays(Column, Column)` — concatenate two arrays without
  dedup (`functions.concat` or equivalent).

`UnionOperator` is refactored to delegate its
`handleOneEmpty` / `handleEquivalentTypes` bodies to these helpers,
preserving all current behaviour and error messages.

### Decision 2: Parser-level desugaring of the function forms

The core architectural choice. Rather than registering `union` /
`combine` as method-defined functions (which cannot deliver the spec's
iteration-context equivalence — see Context), we desugar them at parse
time into the same `EvalOperator` AST that `|` uses.

In `Visitor.visitInvocationExpression`, after building the
`invocationSubject` and `invocationVerb`:

1. If the verb is an `EvalFunction` whose identifier is `union` or
   `combine`, and exactly one argument was provided, synthesise an
   `EvalOperator(invocationSubject, operator, argument)` where
   `operator` is `UnionOperator` (for `union`) or the new
   `CombineOperator` (for `combine`).
2. Any other argument count raises an invalid-user-input error at
   parse time, indicating the function requires exactly one argument.
3. Any other function name falls through to the default
   `invocationSubject.andThen(invocationVerb)` composition.

This makes `x.union(y)` parse to the **same AST** as `x | y`, so
semantic equivalence is guaranteed by construction. `x.combine(y)`
parses to a structurally identical AST with a different merge step.

Consequences:

- Neither `union` nor `combine` appears in the function registry. This
  is consistent with their spec: they have no standalone (subject-less)
  call form.
- Function-lookup consumers (e.g. `fhirpath-lab-server` enumerating
  registered functions) will not list `union` / `combine`. This is
  acceptable: users can still write them, and they behave identically
  to `|`. If a follow-up issue needs to surface these in function
  catalogues, a separate "virtual function" mechanism can be added.
- Error messages for incompatible operand types will read
  `Operator \`|\` is not supported for: ...`(for union, because both
forms share the same AST) and`Operator \`combine\` is not supported
  for: ...`(for combine). The user wrote a function call but sees`Operator ...` in the error — a small UX oddity, flagged as an
  open question but not blocking.

Options considered:

| Option                                                                | Verdict                                                              |
| --------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Parser-level desugaring (Decision 2)                                  | **Chosen** — smallest change that delivers perfect spec equivalence  |
| Regular `MethodDefinedFunction` + accept iteration-context divergence | Rejected — ships a spec deviation that would confuse users           |
| Architectural fix: propagate outer focus through `Composite.apply`    | Rejected for this issue — too broad; tracked as a future improvement |

### Decision 3: Introduce a new `CombineOperator`

Add `CombineOperator` in
`fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/` as a peer
to `UnionOperator`. It extends `SameTypeBinaryOperator` so that it
inherits the exact same empty-operand / reconcile / dispatch template
that `UnionOperator` uses.

- `handleOneEmpty(nonEmpty, input)` returns the non-empty collection
  unchanged (no dedup, no normalisation needed beyond what the
  collection already carries).
- `handleEquivalentTypes(left, right, input)` calls
  `CombiningLogic.combineArrays` after `prepareArray`.
- `getOperatorName()` returns `"combine"`, so failure messages read
  `Operator \`combine\` is not supported for: ...`.
- `handleNonEquivalentTypes` inherits the default from
  `SameTypeBinaryOperator` (throws `InvalidUserInputError`).

`CombineOperator` is **not** wired into the operator grammar (there is
no `combine` symbol in FHIRPath). It is reachable only via the parser
desugaring in Decision 2.

### Decision 4: Test layout — new `CombiningFunctionsDslTest`

The existing `CombiningOperatorsDslTest` is dense and `|`-focused.
Rather than bloat it, add a new `CombiningFunctionsDslTest` that:

1. Mirrors the operator test structure for `union()` across the full
   type matrix (Boolean, Integer, Decimal, String, Date, DateTime,
   Time, Quantity, Coding).
2. Exercises `combine()`'s distinct behaviour (duplicate preservation,
   empty handling, type promotion, error on incompatible types,
   non-dedup for Quantity and Coding).
3. Includes equivalence tests proving `x.union(y)` and `x | y` produce
   equal results across the same type matrix.
4. Covers the iteration-context pin-down cases inside `select`:
   `Patient.name.select(use.union(given))` must equal
   `Patient.name.select(use | given)`, and the `combine` analogue must
   preserve duplicates while still respecting the iteration focus.

These tests are the strongest guarantee that Decision 2's desugaring
actually delivers the promised spec equivalence; the iteration-context
case is the one that would have failed under Option B (regular
function-form).

### Decision 5: YAML exclusions review

Inspect `fhirpath/src/test/resources/fhirpath-js/config.yaml` for any
entries that exclude reference tests solely because `union()` or
`combine()` were not implemented. Any such exclusion is removed as part
of this change. Exclusions that document genuine limitations
(polymorphic unions, calendar duration dedup) remain and apply equally
to the function forms because they share the same AST.

## Risks / Trade-offs

- **[Risk] Refactoring `UnionOperator` regresses `|` behaviour.**
  → Mitigation: the existing exhaustive `CombiningOperatorsDslTest`
  must stay green. No behaviour change is intended; the refactor is
  pure extraction.

- **[Risk] Parser desugaring intercepts too much.** Someone registering
  a future function called `union` or `combine` through the function
  registry would silently be shadowed by the desugaring.
  → Mitigation: document Decision 2 in the design file and in the
  parser visitor. These names are reserved for the spec-defined
  combining semantics, which aligns with the FHIRPath spec's
  reservation of them.

- **[Risk] Error-message UX.** Users writing `x.union(true)` see
  `Operator \`|\` is not supported for: ...`, which may be confusing
  because they called a function.
  → Accepted as a small UX oddity for now. Flagged as an open question.
  A follow-up improvement could preserve the source form through the
  AST for error formatting.

- **[Risk] Function discovery tools miss `union`/`combine`.** External
  tools enumerating `FunctionRegistry` do not see these names.
  → Accepted. Future work can add a virtual-function mechanism if
  needed.

- **[Trade-off] Extra class: `CombineOperator`.** Net code growth is
  small because most behaviour lives in the shared `CombiningLogic`.
  → Accepted.

## Migration Plan

Not applicable — this is a pure addition to the FHIRPath language
surface. No deprecations, no data migration, no configuration changes.
The `|` operator continues to work as before.

## Open Questions

- Should error messages distinguish the function form from the operator
  form (e.g. `Function \`union\` is not supported for: ...`)? This
  requires threading source-form information through the AST. Out of
  scope for this issue; tracked as an open improvement.
- Should `union()` / `combine()` be exposed via a virtual-function
  mechanism so that function-catalogue tooling sees them? Out of scope;
  flagged for future work.
- Does the site documentation page at `site/docs/fhirpath/functions.md`
  (or equivalent) need updating as part of this change, or as a
  follow-up docs PR? Treated as a docs-side task during implementation.
