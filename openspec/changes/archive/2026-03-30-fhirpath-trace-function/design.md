## Context

Pathling translates FHIRPath expressions into Spark SQL column expressions.
The `trace()` function is a diagnostic utility that must produce a logging side
effect during Spark execution while returning the input value unchanged. Spark's
execution model is lazy and distributed, so the logging mechanism must operate
within Catalyst expression evaluation.

The codebase already has a precedent for custom Catalyst expressions:
`PruneSyntheticFields` in the `encoders` module is a `UnaryExpression` with
`CodegenFallback` that transforms values at evaluation time. The same pattern
applies to `trace()`.

## Goals / Non-Goals

**Goals:**

- Implement `trace(name : String)` that logs each evaluated value via SLF4J and
  returns the input collection unchanged.
- Pass existing YAML reference tests for `trace()`, with exclusions only for
  the unsupported projection parameter and the known `.id` extension limitation
  after trace wrapping.
- Provide a testable implementation where log output can be verified in tests.

**Non-Goals:**

- The optional `projection` parameter (deferred to a follow-up).
- FHIR-aware serialisation of logged values (raw `toString()` is sufficient
  initially).
- Performance optimisation of the logging path.

## Decisions

### 1. Custom Catalyst expression over UDF

**Decision:** Implement `TraceExpression` as a Scala `UnaryExpression` with
`CodegenFallback` in the `encoders` module.

**Alternatives considered:**

- **Spark UDF:** UDFs are boxed, lose type information, and require explicit
  registration with the SparkSession. A Catalyst expression preserves the
  child's `dataType` exactly and follows the `PruneSyntheticFields` precedent.
- **No-op pass-through:** Would satisfy the return-value contract but provides
  no diagnostic value to users.

**Rationale:** The Catalyst expression runs inside Spark's evaluation loop,
receives the actual runtime value in `nullSafeEval`, and can log it before
returning unchanged. It preserves the child's data type and nullability without
any conversion overhead.

### 2. Logging via SLF4J on a dedicated logger

**Decision:** Log at `INFO` level using a logger named after the
`TraceExpression` class (e.g.,
`au.csiro.pathling.sql.TraceExpression`).

**Rationale:** SLF4J is already on the classpath. A dedicated logger allows
users to control trace output independently via logback configuration. `INFO`
level is appropriate for diagnostic output that is opt-in by expression authors.

### 3. JSON-like string representation via CatalystTypeConverters

**Decision:** Convert Spark internal values to external Scala types using
`CatalystTypeConverters.createToScalaConverter`, then format as JSON-like
output: `Row.json` for structs, quoted strings for primitives, and recursive
formatting for arrays.

**Alternatives considered:**

- **Raw `toString()`:** Spark internal types (`UnsafeRow`, `ArrayData`) produce
  binary or unhelpful output for complex types, making diagnostics useless.
- **`prettyJson`:** More readable but multi-line output is noisy in logs.

**Rationale:** Compact JSON gives readable, single-line output for all types
including nested FHIR structs. The converter is created lazily and reused per
partition, so the overhead is minimal for a diagnostic function.

### 4. New `UtilityFunctions` provider class

**Decision:** Create `UtilityFunctions.java` in the `function/provider` package,
following the same pattern as `ExistenceFunctions`, `StringFunctions`, etc.

**Rationale:** The FHIRPath spec groups `trace()` under "Utility functions".
A dedicated provider keeps it separate from unrelated function groups and
provides a natural home for future utility functions (e.g., `now()`,
`timeOfDay()`).

### 5. Testing via DatasetEvaluator with ListAppender

**Decision:** Write tests using the `DatasetEvaluator` pattern (as in
`DatasetEvaluatorTest`) which materialises data through Spark. Verify logging
by attaching a Logback `ListAppender` to the `TraceExpression` logger.

**Alternatives considered:**

- **DSL tests only:** Cannot assert on log output without extending the DSL
  builder framework.
- **Unit test on `nullSafeEval` directly:** Would work but doesn't test
  integration through the full FHIRPath → Spark pipeline.
- **SingleResourceEvaluatorTest:** Only builds Column expressions without
  materialising — `nullSafeEval` is never called.

**Rationale:** `DatasetEvaluator` tests run in local Spark mode (single JVM),
so executor logging goes to the same logger and is capturable via
`ListAppender`. This tests both the pass-through semantics and the logging
side effect end-to-end.

## Risks / Trade-offs

- **Distributed logging:** In cluster mode, trace output goes to executor logs,
  not the driver. This is inherent to Spark and acceptable for a diagnostic
  function. → Users can aggregate executor logs or use local mode for debugging.
- **Verbose output for complex types:** Mitigated by using
  `CatalystTypeConverters` with compact JSON serialisation (see Decision #3).
  Deeply nested FHIR structs may still produce long single-line output.
- **Performance:** Logging on every row adds overhead. → `trace()` is
  explicitly a diagnostic function; users choose when to use it.
