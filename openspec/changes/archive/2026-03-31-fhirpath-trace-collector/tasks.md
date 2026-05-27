## 1. TraceCollector Interface and Implementation

- [x] 1.1 Create `TraceCollector` interface in `encoders` module (`au.csiro.pathling.sql` package) with `add(String label, String fhirType, Object value)` method
- [x] 1.2 Create `ListTraceCollector` implementation backed by `ArrayList` (not serializable)
- [x] 1.3 Write unit tests for `ListTraceCollector`

## 2. EvaluationContext Wiring

- [x] 2.1 Add `Optional<TraceCollector> getTraceCollector()` default method to `EvaluationContext` returning `Optional.empty()`
- [x] 2.2 Add optional `TraceCollector` parameter to `FhirEvaluationContext` record and override `getTraceCollector()`
- [x] 2.3 Thread the collector through `SingleResourceEvaluator` and `SingleResourceEvaluatorBuilder`

## 3. TraceExpression Changes

- [x] 3.1 Add `fhirType: String` and `collector: TraceCollector` parameters to `TraceExpression` case class
- [x] 3.2 Update `nullSafeEval` to call `collector.add()` alongside SLF4J logging (null-safe for when no collector is provided)
- [x] 3.3 Update `withNewChildInternal` to preserve the new parameters (handled by case class `copy`)

## 4. UtilityFunctions Changes

- [x] 4.1 Add `EvaluationContext` parameter to `UtilityFunctions.trace()`
- [x] 4.2 Extract FHIR type from input collection and pass to `TraceExpression`
- [x] 4.3 Extract optional `TraceCollector` from context and pass to `TraceExpression`

## 5. SingleInstanceEvaluator Integration

- [x] 5.1 Create `ListTraceCollector` in `SingleInstanceEvaluator.evaluate()` before evaluation
- [x] 5.2 Pass the collector through to the evaluator context
- [x] 5.3 Add `traces` field to `SingleInstanceEvaluationResult` (list of trace entries with label, type, and typed values)
- [x] 5.4 Read collected traces after materialisation and fold into the result DTO
- [x] 5.5 Sanitize `Row` values in trace entries using existing `sanitiseRow()` logic

## 6. Python Bindings

- [x] 6.1 Update `PathlingContext.evaluate_fhirpath()` in Python to read trace data from the Java result
- [x] 6.2 Include `traces` key in the returned Python dict with label, type, and values

## 7. Tests

- [x] 7.1 Test trace collection end-to-end via `DatasetEvaluator` with a `ListTraceCollector`
- [x] 7.2 Test that collector receives correct FHIR types for complex and primitive traced collections
- [x] 7.3 Test that two trace calls produce entries with distinct labels
- [x] 7.4 Test that evaluation without a collector still works (SLF4J only)
- [x] 7.5 Test that `SingleInstanceEvaluationResult` includes trace data (tested via collector integration)
- [x] 7.6 Verify existing `TraceFunctionTest` and YAML reference tests still pass
