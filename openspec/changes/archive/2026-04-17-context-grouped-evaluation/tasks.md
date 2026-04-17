## 1. Result data structure

- [x] 1.1 Create `ResultGroup` class in `fhirpath/evaluation/` with `contextKey` (nullable String), `results` (List\<TypedValue\>), and `traces` (List\<TraceResult\>)
- [x] 1.2 Modify `SingleInstanceEvaluationResult` to replace `results` and `traces` fields with `resultGroups: List<ResultGroup>`. Remove `getResults()` and `getTraces()`, add `getResultGroups()`

## 2. ListTraceCollector changes

- [x] 2.1 Add a `clear()` method to `ListTraceCollector` to reset collected entries between per-element evaluations

## 3. Per-element context evaluation

- [x] 3.1 Rewrite `SingleInstanceEvaluator.evaluateWithContext` to materialise the context array, detect array vs scalar, and iterate per element — evaluating `mainPath` with each element as input context, collecting results and traces per element, resetting the trace collector between elements
- [x] 3.2 Build context keys in the format `contextExpression[index]` for array elements and `contextExpression` for scalar values
- [x] 3.3 Wrap the non-context path in `evaluate()` to return a single `ResultGroup` with null `contextKey`

## 4. Update callers

- [x] 4.1 Update `PathlingContext.evaluateFhirPath` to work with the new `SingleInstanceEvaluationResult` structure (no signature change needed — the return type is the same class)
- [x] 4.2 Update Python `evaluate_fhirpath` in `lib/python/pathling/context.py` to return `resultGroups` list of dicts instead of top-level `results`/`traces`

## 5. Tests

- [x] 5.1 Update existing tests in `EvaluateFhirPathTest` to access results via `getResultGroups().getFirst().getResults()` etc.
- [x] 5.2 Add test for context evaluation returning multiple `ResultGroup` entries with correct context keys
- [x] 5.3 Add test for per-element trace isolation — verify each `ResultGroup` contains only its own traces
- [x] 5.4 Add test for empty context expression producing zero result groups
- [x] 5.5 Add test for scalar context expression producing a single `ResultGroup` with unindexed context key
- [x] 5.6 Update Python tests if present to use the new `resultGroups` structure
