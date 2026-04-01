## Context

The `%rowIndex` environment variable is already implemented for `forEach`/`forEachOrNull` via `UnnestingSelection`. That implementation uses Spark's two-argument `transform(array, (element, index) -> ...)` lambda, which provides the element index natively. The index column is threaded through `ProjectionContext.withRowIndex()` and injected into expression evaluation via `SingleResourceEvaluator.withVariable()`.

`RepeatSelection` works differently. It uses `ValueFunctions.transformTree()` to recursively flatten a tree structure (e.g., nested extensions) by concatenating results across depth levels and traversal branches. The `transformTree` function internally uses `Concat` to merge arrays from each depth level. There is no built-in Spark mechanism to track a global position index across this flattened concatenation.

## Goals / Non-Goals

**Goals:**

- Provide `%rowIndex` as a 0-based integer within `repeat` iterations, reflecting the element's position in the flattened traversal-order output.
- Reset the counter to 0 for each resource row.
- Scope `%rowIndex` to the nearest enclosing iteration directive (`repeat`, `forEach`, or `forEachOrNull`), so nested directives maintain independent indices.

**Non-Goals:**

- Changing how `%rowIndex` works for `forEach`/`forEachOrNull` (already implemented).

## Decisions

### Use a stateful counter expression for global traversal-order indexing

**Decision:** Introduce a `RowIndexCounter` class (thread-safe via `ThreadLocal`) and two Spark expressions — `RowCounter` (returns current value and increments) and `ResetCounter` (resets to 0 before evaluating its child). The counter is shared across the entire `transformTree` invocation, producing a monotonically increasing sequence across all depth levels and branches.

**Rationale:** Unlike `forEach` where Spark's indexed `transform` provides per-element indices natively, `transformTree` concatenates results from multiple recursive levels. No single Spark `transform` call sees the full flattened output. A stateful counter that increments on each element evaluation is the simplest way to produce a global traversal-order index.

**Alternatives considered:**

- _Post-hoc indexing with `posexplode`_: Explode the final array with position indices. Rejected because the array is already embedded in a column expression pipeline — adding a dataset-level operation would require restructuring the projection architecture.
- _Pre-stamping indices into the tree_: Wrap each element with its index before `transformTree`. Rejected because the total count across levels is not known until traversal completes, and the breadth-first concatenation order makes pre-computation complex.

### Thread-safety via ThreadLocal

**Decision:** `RowIndexCounter` uses `ThreadLocal<int[]>` for its mutable state. The class is `Serializable` with a transient `ThreadLocal` field that is lazily re-initialized after deserialization.

**Rationale:** Spark tasks run in parallel across partitions on different threads. `ThreadLocal` ensures each partition's task thread gets an independent counter, preventing cross-partition interference. The `int[]` wrapper avoids boxing overhead. Lazy re-initialization handles the case where the counter is serialized to an executor and deserialized in a new JVM.

### Inject counter via ProjectionContext.withRowIndex()

**Decision:** `RepeatSelection` creates a `RowIndexCounter`, wraps it in a `RowCounter` column, and injects it into the `ProjectionContext` via the existing `withRowIndex()` method before building the `transformTree` expression. The final result is wrapped with `ResetCounter` to ensure the counter resets for each resource row.

**Rationale:** This reuses the same mechanism that `UnnestingSelection` uses for `forEach` — the `rowIndex` field on `ProjectionContext` is already threaded into `evalExpression()` and resolved as the `%rowIndex` variable. The only difference is the source of the index column: Spark's indexed transform lambda vs. a stateful counter expression.

**Scoping:** When a `forEach` is nested inside a `repeat`, the inner `UnnestingSelection` calls `withRowIndex(index)` with its own transform-provided index, naturally shadowing the outer `repeat`'s counter. Conversely, a `repeat` nested inside a `forEach` would create its own `RowIndexCounter`, independent of the outer scope.

### Place RowCounter/ResetCounter in the encoders module

**Decision:** The `RowCounter` and `ResetCounter` Spark expressions, along with the `RowIndexCounter` state class, are placed in the `encoders` module alongside other custom Spark expressions (`Expressions.scala`, `ValueFunctions.java`).

**Rationale:** The `encoders` module already contains all custom Spark Catalyst expressions (e.g., `TransformTree`, `PruneSyntheticFields`). `RowCounter` and `ResetCounter` are general-purpose Spark expressions that could potentially be reused beyond `repeat`. Convenience methods are added to `ValueFunctions` following the existing pattern.

## Risks / Trade-offs

**[Risk] Evaluation order determinism** — The counter relies on deterministic evaluation order within `transformTree`. If Spark were to evaluate elements in a non-deterministic order, indices would be unpredictable.
→ **Mitigation:** `transformTree` uses `Concat` of `transform` calls, both of which preserve array order. Spark's higher-order functions evaluate elements sequentially within a single row. The spike's tests confirm deterministic ordering.

**[Risk] Single-partition constraint in tests** — The spike's encoder unit tests use `.repartition(1)` to ensure deterministic evaluation order across rows. This is a test-level constraint, not a runtime limitation — in production, each partition processes its rows independently and the counter resets per row via `ResetCounter`.
→ **Mitigation:** Document this constraint in test comments. The `ResetCounter` ensures correctness regardless of partitioning.

**[Risk] Codegen compatibility** — `RowCounter` extends `Nondeterministic` and implements `doGenCode` for Spark's whole-stage code generation. If the codegen path diverges from the interpreted path, indices could be incorrect.
→ **Mitigation:** The `ExpressionsBothModesTest` base class runs all encoder tests in both interpreted and codegen modes, catching any divergence.
