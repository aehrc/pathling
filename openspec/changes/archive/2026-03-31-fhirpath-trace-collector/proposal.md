## Why

The FHIRPath `trace()` function currently logs to SLF4J but discards the traced
values. The primary consumer — fhirpath-lab — needs to return trace data
alongside evaluation results so users can inspect intermediate collections in the
web UI. The `SingleInstanceEvaluationResult` DTO must carry trace entries back
through the Python API to the fhirpath-lab web service.

## What Changes

- Introduce a `TraceCollector` interface with a simple list-backed
  implementation for capturing trace entries (label, FHIR type, values) during
  expression evaluation.
- Thread the collector through `EvaluationContext` so `UtilityFunctions.trace()`
  can write to it alongside the existing SLF4J logging.
- Pass the FHIR type from the input `Collection` into `TraceExpression` so
  collected entries carry type metadata.
- Extend `SingleInstanceEvaluationResult` with a list of trace entries, each
  containing the trace label, FHIR type, and list of typed values.
- Update `SingleInstanceEvaluator` to create a collector before evaluation,
  attach it to the configuration/context, and fold the collected traces into
  the result DTO after materialization.

## Capabilities

### New Capabilities

- `fhirpath-trace-collector`: The `TraceCollector` interface, its list-backed
  implementation, the `TraceEntry` data class, and the wiring through
  `EvaluationContext` into `TraceExpression`.

### Modified Capabilities

- `fhirpath-trace`: Trace now supports programmatic collection via
  `TraceCollector` in addition to SLF4J logging. `TraceExpression` gains
  `fhirType` and `collector` parameters.
- `fhirpath-evaluation-api`: `SingleInstanceEvaluationResult` gains a `traces`
  field containing collected trace entries with label, type, and values.

## Impact

- `encoders` module: `TraceExpression.scala` gains `fhirType` and `collector`
  parameters.
- `fhirpath` module: new `TraceCollector` interface and `ListTraceCollector`
  implementation; changes to `EvaluationContext`, `FhirpathConfiguration` or
  `FhirEvaluationContext`; changes to `UtilityFunctions.trace()`;
  changes to `SingleInstanceEvaluator` and `SingleInstanceEvaluationResult`.
- `library-api` module: `PathlingContext.evaluateFhirPath()` returns the
  enriched result.
- `lib/python` module: `evaluate_fhirpath()` returns trace data in the result
  dict.
- Downstream: fhirpath-lab-api can read trace entries from the result and build
  the FHIR Parameters response.
