## Context

Pathling's ViewDefinition processing uses `UnnestingSelection` to implement `forEach`/`forEachOrNull`. This evaluates a FHIRPath expression to get an array-valued collection, then applies a projection to each element using Spark's higher-order `transform(array, element -> ...)` function via `ColumnRepresentation.transform()` and `ProjectionClause.evaluateElementWise()`.

FHIRPath environment variables are resolved through a `VariableResolverChain` — a chain-of-responsibility pattern where resolvers (`BuiltInConstantResolver`, `ContextVariableResolver`, `SuppliedVariableResolver`, etc.) are queried in sequence. Variables resolve to `Collection` objects containing Spark `Column` expressions. User-supplied variables (e.g. ViewDefinition constants) are passed as `Map<String, Collection>` through the `SingleResourceEvaluator`.

The `%rowIndex` variable is different from existing environment variables because its value changes per-element during iteration, rather than being constant across the entire evaluation.

## Goals / Non-Goals

**Goals:**

- Provide `%rowIndex` as a 0-based integer environment variable within `forEach` and `forEachOrNull` iterations.
- Default to `0` at the top level when no iteration is active.
- Support independent `%rowIndex` values at each nesting level.
- Make `%rowIndex` available to all FHIRPath expressions within the iteration scope.

**Non-Goals:**

- Supporting `%rowIndex` within `repeat` iterations (separate future work).
- Changes to the FHIRPath parser or grammar (environment variables are already parsed via the `%name` syntax).

## Decisions

### Use Spark's indexed transform for per-element index tracking

**Decision:** Use `functions.transform(array, (element, index) -> ...)` — Spark's two-argument lambda variant of the `transform` higher-order function — to propagate the element index during unnesting.

**Rationale:** The current `evaluateElementWise` method uses `ColumnRepresentation.transform()` which calls `functions.transform(array, element -> ...)`. Spark provides a built-in overload that passes both the element and its 0-based index to the lambda. This aligns directly with the `%rowIndex` semantics and avoids generating indices externally.

**Alternatives considered:**

- _`posexplode` + rejoin_: Would explode arrays with position indices then rejoin. Rejected because it requires dataset-level operations (adding/removing rows), which conflicts with the current column-expression-based architecture that works within Spark's higher-order functions.
- _`zip_with_index` preprocessing_: Would pre-wrap each array element with its index before transformation. Rejected as unnecessary complexity when Spark's `transform` already provides index natively.

### Inject %rowIndex via the existing supplied variables mechanism

**Decision:** Pass `%rowIndex` as a supplied variable in the `Map<String, Collection>` that flows through `SingleResourceEvaluator`. The `UnnestingSelection` will create a new evaluator (or updated variable map) for each unnesting level that includes the current index column as the `rowIndex` variable.

**Rationale:** The existing `SuppliedVariableResolver` and `VariableResolverChain` infrastructure already supports arbitrary named variables passed as `Collection` objects. Using this mechanism avoids creating a new resolver type and keeps `%rowIndex` consistent with how ViewDefinition constants are handled.

**Alternative considered:**

- _Dedicated `RowIndexResolver`_: A new `EnvironmentVariableResolver` implementation specific to `%rowIndex`. Rejected as over-engineering — the supplied variables mechanism handles this cleanly and requires no changes to the resolver chain infrastructure.

### Thread index through ProjectionContext

**Decision:** Extend `ProjectionContext` to carry the current `%rowIndex` column, and update `UnnestingSelection` to pass the index from Spark's `transform` lambda into the projection context. The context will merge this index into the evaluator's variable map when creating the per-element evaluation context.

**Rationale:** `ProjectionContext` is the natural place to carry per-iteration state since it already carries the `inputContext` and `evaluator`. Adding the row index here keeps the change localised to the projection layer and avoids threading index state through unrelated components.

**Implementation approach:**

1. Add a `rowIndex` field (type `Column`, defaulting to `lit(0)`) to `ProjectionContext`.
2. Modify `evaluateElementWise` in `UnnestingSelection` (or introduce a new method) to use the indexed `transform` variant, capturing the index column.
3. When building the per-element `ProjectionContext`, include the index column as a `rowIndex` supplied variable via the evaluator's variable map.
4. Nesting is handled naturally: each `UnnestingSelection` creates a new context with its own `%rowIndex`, shadowing the outer value.

### Use IntegerCollection for the %rowIndex type

**Decision:** Represent `%rowIndex` as an `IntegerCollection` wrapping a Spark integer column.

**Rationale:** `IntegerCollection` is the standard FHIRPath integer representation. It supports arithmetic (`%rowIndex + 1`) and comparisons (`%rowIndex = 0`) out of the box. The index from Spark's `transform` is already an integer column, so no type conversion is needed.

## Risks / Trade-offs

**[Risk] Evaluator immutability** — `SingleResourceEvaluator` stores variables as a `Map<String, Collection>` set at construction time. Injecting a per-element `%rowIndex` requires either creating a new evaluator per unnesting level or making the variable map mutable.
→ **Mitigation:** Create a new `SingleResourceEvaluator` (or a lightweight wrapper) per `UnnestingSelection` that includes `rowIndex` in its variable map. This preserves immutability and isolates each nesting level.

**[Risk] Performance impact of creating per-level evaluators** — Creating new evaluator instances per unnesting level could add overhead.
→ **Mitigation:** The evaluators are lightweight objects (no dataset or SparkSession state). The per-level cost is negligible compared to the Spark query execution itself. Additionally, this already happens implicitly via `ProjectionContext.withInputContext()`.

**[Risk] forEachOrNull with empty collection should produce %rowIndex = 0** — When `forEachOrNull` produces a null row for an empty collection, the index must still resolve to `0`.
→ **Mitigation:** The `orNull` mechanism in `ProjectionResult` handles the empty-collection case. The default `%rowIndex` value of `0` in the projection context will naturally apply since no transform iteration occurs for empty collections.
