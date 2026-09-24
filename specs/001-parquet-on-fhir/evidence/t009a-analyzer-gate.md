# The analyzer gate

T009a, the Phase 1 risk gate. R-005 records one residual risk in the chosen
design: `RuntimeReplaceable.dataType` delegates to `replacement`, so a path
that probes `dataType` before the child resolves would force the replacement
early — and force it against an unresolved child, which either throws or caches
a wrong answer in the `lazy val`. R-017 routes a failure to option C, the
unbound column representation, rewriting T038e, T038l, T110, T113a and T113b
before M2 opens.

**Result: the gate passes.** Across 20 distinct runs — nine plan shapes under
each of two child constructions, plus two `transform` lambda shapes whose child
is fixed by construction — nothing forced `replacement` before the child
resolved, nothing threw, and every plan optimised to the same shape a
statically written traversal would have produced. The design stays on
option D. T038a remains the durable test in Phase 8; this is the throwaway
spike that bought the answer while M1 still had planning time.

## Conditions

| | |
| --- | --- |
| Commit | `b32f6b5479` |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 26.6.2 |
| JVM | OpenJDK 21.0.3+7-LTS-152, HotSpot 64-Bit Server VM |
| Spark | 4.0.2 (Scala 2.13) |
| Session | `local[2]`, ANSI mode on (the supported default) |

The spike code is not in the repository. It was written into
`encoders/src/test/`, run, and deleted; the transcript it produced is
`t009a-analyzer-gate.txt`, beside this file.

## What was built

The expression under test, in its minimal form:

```scala
case class SpikeResolveOrNull(child: Expression, fieldName: String, fallback: DataType)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = child.dataType match {
    case s: StructType if s.fieldNames.contains(fieldName) =>
      GetStructField(child, s.fieldIndex(fieldName), Some(fieldName))
    case _ =>
      Literal(null, fallback)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
```

Three construction choices are load-bearing, and T038e must repeat them.

- **`UnaryLike`, not `InheritAnalysisRules`.** `InheritAnalysisRules` makes
  `child` the *replacement*, so the analyzer would resolve inside the
  replacement and the traversal target would never be a child at all. The
  traversal target must be the child, so that resolving it is what the analyzer
  does.
- **`replacement` is a `lazy val` on a case class.** Resolution replaces the
  child, and `withNewChildren` returns a `copy`, so the instance whose
  replacement is eventually forced is a fresh one. This is what makes a
  premature forcing harmless in principle — but only if it is a copy, which is
  why the construction has to be this shape.
- **The replacement is built only from resolved leaves** (`GetStructField`,
  `Literal`). `CheckAnalysis` rejects a `RuntimeReplaceable` whose
  replacement is unresolved, so building it over, say,
  `UnresolvedExtractValue` would fail the gate for a reason that has nothing
  to do with the question being asked.

## How it was measured

The direct measurement, rather than an inference from the result: the expression
records every forcing of `replacement` together with `child.resolved` at that
moment. A premature probe is therefore visible even in the case where it does
*not* throw — the silent one, where a `lazy val` caches a fallback that the
resolved child would not have produced.

Each scenario also captures the analyzed plan, the optimised plan, the result
schema and the collected rows.

**The positive control**: the child must not have been bound to the dataset
before the analyzer ran. Building it with `df.col("s")` instead of
`functions.col("s")` would hand over an `AttributeReference` and the gate
would pass without testing anything, so every scenario asserts the child is not
an `AttributeReference` at construction.

Two child shapes were run, because they resolve by different routes:

- **`UnresolvedAttribute`** — the Catalyst-level case the spec names. Reports
  `resolved == false`.
- **`ColumnNodeExpression`** — what `ExpressionUtils.expression(column)`
  actually returns under Spark 4's `ColumnNode` API, and therefore what engine
  code will hand the expression in practice. **It reports
  `resolved == true` while still wrapping an unresolved node**, so
  `child.resolved` cannot be used as a guard. Both shapes pass, but this one
  is worth knowing: `resolved()` on the wrapper is not meaningful at
  construction time, so a caller cannot inspect it to decide anything. It is
  *not* a hazard inside the expression — the transcript shows the wrapper is
  replaced before the analyzer reaches the node, so every forcing in this mode
  saw an `AttributeReference` and an honest `child.resolved`.

## Scenarios

Nine run under both child shapes, plus two whose child is an
`UnresolvedNamedLambdaVariable` by construction and so are mode-independent —
they run identically under both and count once each.

| Scenario | Why |
| --- | --- |
| present / select | The base case. |
| absent / select | The fallback case. |
| present / filter | Type coercion on a binary operator probes `dataType`. |
| present / groupBy | Grouping forces `semanticEquals` → `canonicalized` → `replacement`. This was the likeliest place to bite. |
| present / orderBy | Sort ordering probes the type. |
| absent / groupBy | The fallback under the same pressure. |
| nested present-over-present | R-005 emits one at *every* traversal step, so a spike over a spike must work. |
| nested present-over-absent-struct | The outer step over an inner that fell back to a struct-typed null. |
| nested present-over-absent-void | The outer step over an inner that fell back to the bottom type, which is not a struct at all — this must fall back rather than raise `MatchError`. |
| transform lambda / present *(mode-independent)* | **The latest-resolving child the engine produces.** `ResolveLambdaVariables` runs after `ResolveReferences`, so a lambda variable stays unresolved for strictly longer than an attribute does, and the engine wraps every repeating element in `transform`. |
| transform lambda / absent *(mode-independent)* | The same, falling back. |

## What the transcript shows

Every forcing log entry reads `childResolved=true`, and the child is always an
`AttributeReference`, a `NamedLambdaVariable`, or an already-resolved inner
`SpikeResolveOrNull`. Nothing probed `dataType` early, under any
plan shape, under either child construction.

The analyzed plan still carries the node with its child resolved, which is the
proof that the replacement was *not* forced during analysis:

    Project [spikeresolveornull(s#5, b, StringType) AS spikeresolveornull(s)#6]

The optimised plan has the node gone and the direct access in its place —
`ReplaceExpressions` running ahead of the pruning rules, as finding 14
measured:

    Project [cast(id#0L as string) AS spikeresolveornull(s)#6]

Three details in the optimised plans are worth keeping.

- **Present, selected**: the struct construction collapses entirely and the
  plan reads `cast(id#0L as string)` — indistinguishable from a statically
  written field reference, which is the property FR-054 depends on.
- **Absent, grouped**: `Aggregate [0], [null AS g#62, ...]` over an empty
  `Project`. The typed null folds away and the parent column is not read at
  all, so an absent element costs nothing at scan time.
- **Lambda**: `lambdafunction(lambda x#114.b, lambda x#114, false)` for the
  present case and `lambdafunction(null, ...)` for the absent one. The
  fallback survives `transform` and yields `array<string>`.

## What this does not answer

The scope here is the analyzer question alone. The widening behaviour of the
fallback types — `void` and `array<void>` against `coalesce` and
`transform` — is already measured in R-005 (findings 14–15) and is tested by
T038c. This spike says nothing about it, and its `transform` scenarios use a
concrete `StringType` fallback rather than a bottom type, so they are not
evidence about `array<void>`.

The spike also proves nothing about a *fitted* schema derived from real
definitions: the fixtures are hand-built structs. That is T038b's job.
