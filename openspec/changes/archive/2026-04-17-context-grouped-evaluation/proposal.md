## Why

The FHIRPath Lab "Context Expression" field is expected to group results by
context element — evaluating the main expression once per element returned by
the context expression, with results and traces scoped to each element. The
current Pathling implementation composes context and main expressions into a
single evaluation, returning a flat list with no grouping. This prevents
FHIRPath Lab from displaying per-element results, traces, and context keys
(e.g., `name[0]`, `name[1]`).

See: https://github.com/aehrc/pathling/issues/2586

## What Changes

- **BREAKING**: Replace flat `results`/`traces` fields on
  `SingleInstanceEvaluationResult` with a uniform `resultGroups` list of
  `ResultGroup` objects. Each `ResultGroup` contains a context key, results,
  and traces. Non-context evaluation returns a single `ResultGroup` with a null
  context key.
- Implement per-context-element evaluation in `SingleInstanceEvaluator`, where
  the main expression is evaluated independently for each context element with
  isolated trace collection.
- **BREAKING**: Update the Python `evaluate_fhirpath` return structure to use
  `contextResults` (list of groups) instead of top-level `results`/`traces`.
- Update existing tests to use the new result structure.

## Capabilities

### New Capabilities

- `context-grouped-evaluation`: Requirements for per-context-element grouping
  of evaluation results, including the `ResultGroup` data structure, context
  key format, per-element trace isolation, and backward-compatible handling of
  the no-context case.

### Modified Capabilities

- `fhirpath-evaluation-api`: The context expression evaluation scenario changes
  from flat results to grouped results. The result data structure changes from
  top-level `results`/`traces` to a uniform `resultGroups` list. Python
  bindings return structure changes accordingly.

## Impact

- **Java API**: `SingleInstanceEvaluationResult` breaking change — callers must
  access results via `getResultGroups()` instead of `getResults()`/`getTraces()`.
- **Python API**: `evaluate_fhirpath` return dict structure changes.
- **Modules affected**: `fhirpath` (evaluator, result classes), `library-api`
  (PathlingContext), `lib/python` (context.py).
- **No impact** on server, R bindings, encoders, or terminology modules.
