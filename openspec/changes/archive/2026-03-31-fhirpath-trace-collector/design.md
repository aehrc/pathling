## Context

The `trace()` function (issue #2580) currently logs values to SLF4J via
`TraceExpression` but discards them. The fhirpath-lab web service needs to
return trace data alongside evaluation results. The evaluation chain is:

```
fhirpath-lab API (Python web service)
  → PathlingContext.evaluate_fhirpath() [Python, py4j]
    → PathlingContext.evaluateFhirPath() [Java, library-api]
      → SingleInstanceEvaluator.evaluate() [Java, fhirpath]
        → TraceExpression.nullSafeEval() [Scala, encoders]
```

The `FunctionParameterResolver` already auto-injects `EvaluationContext` into
`@FhirPathFunction` methods (see `repeat()` for precedent). The
`EvaluationContext` carries `FhirpathConfiguration` which is accessible from
all evaluation participants.

`SingleInstanceEvaluator` always operates on a single-row dataset in local Spark
mode — no distributed serialization concerns.

## Goals / Non-Goals

**Goals:**

- Provide a `TraceCollector` interface for programmatic capture of trace entries
  during evaluation.
- Thread the collector through the existing `EvaluationContext` so
  `UtilityFunctions.trace()` can populate it.
- Enrich `SingleInstanceEvaluationResult` with collected trace data.
- Preserve existing SLF4J logging behaviour — collection is additive, not a
  replacement.

**Non-Goals:**

- Accumulator-backed implementation for distributed mode (future work).
- Formatting trace data as FHIR Parameters (fhirpath-lab-api responsibility).
- Changes to the fhirpath-lab-api Python web service.
- Support for the `projection` parameter of `trace()`.

## Decisions

### 1. TraceCollector as an interface

**Decision:** Define `TraceCollector` as an interface with a
`ListTraceCollector` implementation backed by a plain `ArrayList`.

**Alternatives considered:**

- **Concrete class only:** Less flexible, harder to swap for an
  accumulator-backed implementation later.
- **Spark AccumulatorV2:** Proper Spark pattern for collecting side-effect data,
  but requires SparkContext for registration and adds serialization ceremony.
  Overkill for the single-row local-mode use case.

**Rationale:** An interface allows swapping in an `AccumulatorV2`-backed
implementation in the future without changing callers. The list-backed
implementation is intentionally not serializable — it will fail fast with a
clear error if used in a distributed context, rather than silently losing data.

### 2. Collector lives on EvaluationContext

**Decision:** Add an `Optional<TraceCollector> getTraceCollector()` method to
`EvaluationContext` with a default returning `Optional.empty()`.
`FhirEvaluationContext` accepts an optional collector at construction.

**Alternatives considered:**

- **On FhirpathConfiguration:** Configuration is a data class (Lombok
  `@Builder`). Adding a mutable collector to an immutable configuration object
  is semantically wrong.
- **Thread-local global:** Works but invisible coupling, hard to test, lifecycle
  management issues.

**Rationale:** `EvaluationContext` is already the injection mechanism for
contextual state into function providers. The `FunctionParameterResolver`
auto-injects it into `@FhirPathFunction` methods that declare it as a
parameter. The collector naturally belongs with other evaluation-scoped state.

### 3. TraceExpression receives fhirType and collector

**Decision:** Extend `TraceExpression` with two new parameters:

```scala
case class TraceExpression(
  child: Expression,
  name: String,
  fhirType: String,
  collector: TraceCollector
)
```

`UtilityFunctions.trace()` extracts the FHIR type from `input.getFhirType()`
and the collector from `EvaluationContext`. If no collector is present, create
the expression without one (or use a no-op implementation) and rely on SLF4J
logging only.

**Rationale:** The FHIR type is only known at the `Collection` level (not at
the Catalyst expression level). Passing it into the expression at construction
time is the simplest way to propagate it. The collector reference is not
serializable, which is intentional — see Decision #1.

### 4. TraceEntry carries label, FHIR type, and typed values

**Decision:** Each `TraceEntry` contains:

- `label` (String) — the trace name argument
- `fhirType` (String) — the FHIR type code (e.g., `"HumanName"`, `"string"`)
- `values` (List&lt;Object&gt;) — the converted Scala values (Row for structs,
  String/Integer/etc. for primitives)

Multiple `nullSafeEval` calls with the same label accumulate values into a
single entry (grouped by label).

**Rationale:** This mirrors the response format needed by fhirpath-lab, where
each trace part groups values under a label. The fhirpath-lab-api converts
these to FHIR Parameters parts.

### 5. SingleInstanceEvaluator creates and reads the collector

**Decision:** `SingleInstanceEvaluator.evaluate()` creates a
`ListTraceCollector` before evaluation, passes it through the
`FhirEvaluationContext` constructor, and reads collected traces after
materialization. The traces are included in `SingleInstanceEvaluationResult`.

**Rationale:** This scopes the collector lifecycle to a single evaluation call.
No global state, no cleanup needed. The caller (PathlingContext, Python API)
just reads traces from the result DTO.

### 6. Row sanitization and JSON conversion on the Java side

**Decision:** Trace values that are `Row` objects are sanitized using the
existing `SingleInstanceEvaluator.sanitiseRow()` logic (strips synthetic fields
and null values) and then converted to JSON strings via `rowToJson()`, consistent
with how main result values are handled.

**Alternatives considered:**

- **Raw Rows:** Would expose internal fields like `_fid`, `_value_scale`, etc.
- **Sanitized Rows without JSON:** Would require consumers to call `Row.json()`
  across the Py4J bridge, which is fragile.

**Rationale:** Converting to JSON strings on the Java side means the Python API
receives plain strings that can be directly included in the FHIR Parameters
response. This is consistent with how `convertValue()` handles main result Rows.

### 7. TraceCollectorProxy for Spark serialization

**Decision:** Introduce `TraceCollectorProxy`, a serializable `TraceCollector`
implementation that delegates to a real collector via a static
`ConcurrentHashMap` registry keyed by UUID. The proxy holds only the UUID string
(serializable). `SingleInstanceEvaluator` creates a `ListTraceCollector` and
wraps it in a proxy before passing it into the evaluation context. After
materialization, the proxy is closed (deregistered).

**Alternatives considered:**

- **`@transient` collector field on TraceExpression:** Would silently lose trace
  data after deserialization rather than collecting it.
- **Making ListTraceCollector serializable:** The deserialized copy would be a
  different object than the caller reads from — collected entries would be lost.
- **Spark AccumulatorV2:** Correct Spark pattern but requires SparkContext
  registration and is overkill for local mode.

**Rationale:** Spark serializes expression plans even in local mode (for closure
cleaning). The proxy pattern keeps the real collector on the driver JVM and
survives serialization by holding only a registry key. The `AutoCloseable`
interface ensures cleanup after each evaluation.

## Risks / Trade-offs

- **Non-serializable collector behind proxy:** The `ListTraceCollector` itself is
  not serializable, but it is never passed directly into Spark expressions. The
  `TraceCollectorProxy` handles serialization transparently. If someone bypasses
  the proxy and passes `ListTraceCollector` directly, Spark will fail with a
  clear serialization error.
  → Accumulator implementation can be added later behind the same interface.
- **Memory usage:** All trace values are held in memory during evaluation.
  → Acceptable for single-resource evaluation. Not intended for batch use.
- **Breaking change to TraceExpression constructor:** Adding parameters changes
  the case class signature.
  → Internal API, not user-facing. All call sites are in `UtilityFunctions`.
