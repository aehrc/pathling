## Context

The `SingleInstanceEvaluator` evaluates FHIRPath expressions against a single
encoded FHIR resource. When a context expression is provided (e.g., `name`),
the current implementation composes the context and main expressions via
`contextPath.andThen(mainPath)` and evaluates once, returning a flat list of
results and traces with no per-element grouping.

The FHIRPath Lab expects results grouped by context element — each element
identified by a key like `name[0]`, with its own expression results and trace
outputs.

Key architectural constraints:

- The FHIRPath evaluator produces lazy Spark `Column` expressions, not
  materialised values.
- `Collection` objects wrap Columns and may represent arrays or scalars — the
  distinction is resolved at Spark plan time via `vectorize`/`ifArray`, not at
  the Java level.
- Trace collection uses a side-channel (`ListTraceCollector`) that accumulates
  entries during Spark execution via `TraceExpression.evalInternal`.
- This is single-resource evaluation (one row), so Spark action overhead is the
  primary performance concern, not data volume.

## Goals / Non-Goals

**Goals:**

- Per-context-element result grouping with context keys, results, and traces.
- Uniform result structure (`ResultGroup`) for both context and non-context
  evaluation.
- Updated Python API that mirrors the Java structure.

**Non-Goals:**

- Changing the projection/view infrastructure (SQL on FHIR views).
- Optimising for large context cardinalities (hundreds of elements).
- R bindings update (deferred to a separate change).

## Decisions

### 1. Uniform result structure with `ResultGroup`

`SingleInstanceEvaluationResult` will replace the top-level `results` and
`traces` fields with a single `resultGroups: List<ResultGroup>` field.
`ResultGroup` contains `contextKey` (nullable String), `results`
(List\<TypedValue\>), and `traces` (List\<TraceResult\>).

Non-context evaluation returns one `ResultGroup` with `contextKey = null`.
Context evaluation returns N groups with keys like `name[0]`.

**Rationale:** A uniform structure avoids conditional shapes in the API. Callers
always iterate `resultGroups` regardless of whether a context was provided. This
is cleaner than an either/or design with separate fields for the two cases.

**Alternative considered:** Additive design — keep existing `results`/`traces`
fields and add an optional `contextResults` field. Rejected because it creates
an ambiguous API where callers must know which fields to read based on whether
context was provided, and the old fields would be vestigial for context
evaluation.

### 2. Per-element iteration with N+1 Spark actions (Option A)

The context array is materialised via a single Spark action to determine N.
Then the main expression is evaluated independently for each context element
(N actions), with the trace collector reset between elements.

Per-element evaluation uses `contextArray.getItem(i)` to create a Column for
element i, wraps it in a `Collection` via `copyWithColumn`, and passes it as
input context to `evaluator.evaluate(mainPath, elementContext)`.

**Rationale:** Simple implementation with clean trace isolation per element. No
new Catalyst expressions or thread-safety machinery required. For single-resource
evaluation with typical context cardinalities (2-10 elements), the overhead of
N+1 Spark actions is acceptable.

**Alternative considered (Option B — documented for future optimisation):**
Use Spark's `transform(array, (elem, idx) -> ...)` higher-order function to
evaluate all elements in a single Spark action, consistent with the
`evaluateElementWise` pattern in the projection package. A new
`TraceContextExpression` Catalyst expression would set the element index on the
trace collector (via a `ThreadLocal` inside the collector) so that
`TraceExpression` entries can be partitioned by element after collection. This
approach avoids N+1 actions and handles array-vs-scalar naturally via
`vectorize`, but introduces a new Catalyst expression and thread-local coupling
between expression nodes. It is the recommended upgrade path if context
evaluation performance becomes a concern.

### 3. Context key format

Context keys use the format `contextExpression[index]` — e.g., `name[0]`,
`contact.telecom[2]`. The index is zero-based and corresponds to the position
in the context array. When context evaluation produces a single scalar (not an
array), the key is the context expression without an index.

### 4. Array detection and element count

Before per-element iteration, the context Column is materialised via
`resourceDf.select(contextColumn).collectAsList()`. The raw value is inspected:

- If it is a `scala.collection.Seq`, it is an array — N is the sequence length.
- Otherwise, it is a scalar — treated as a single element (N=1) with no index
  in the context key.

### 5. Python API mirrors Java structure

The Python `evaluate_fhirpath` return dict changes from:

```python
{"results": [...], "expectedReturnType": "...", "traces": [...]}
```

to:

```python
{
    "expectedReturnType": "...",
    "resultGroups": [
        {"contextKey": None, "results": [...], "traces": [...]}
    ]
}
```

No convenience wrappers for the no-context case — callers use the uniform
structure.

## Risks / Trade-offs

**[N+1 Spark actions for large contexts]** → For context expressions returning
many elements (e.g., `Observation.component` with 20+ items), the per-element
evaluation incurs noticeable overhead. Mitigation: document Option B as the
upgrade path. Typical FHIRPath Lab usage involves small contexts (2-5 elements).

**[Array vs scalar detection relies on materialised value inspection]** → The
code must inspect the raw Spark value type (`scala.collection.Seq` vs other) to
distinguish arrays from scalars. This is the same pattern used by
`materialiseValues` in the current codebase, so it is well-established.

**[Breaking change to result structure]** → All callers of
`SingleInstanceEvaluationResult` must be updated. Mitigation: the only callers
are `PathlingContext` (Java), `context.py` (Python), and the fhirpath-lab-api
server. All are updated in this change.

**[Trace collector reset between elements]** → The `ListTraceCollector` must be
cleared between per-element evaluations to isolate traces. A `clear()` method
will be added. Risk of leaked state if an evaluation throws before reset.
Mitigation: use try/finally to ensure cleanup.
