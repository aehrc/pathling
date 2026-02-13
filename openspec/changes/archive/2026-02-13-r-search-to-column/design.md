## Context

The Java `PathlingContext.searchToColumn(resourceType, searchExpression)` method
already exists and returns a Spark `Column`. The Python library wraps it via
`self._jpc.searchToColumn(...)` and returns a PySpark `Column`. The R library
uses sparklyr's `j_invoke` to call Java methods on the `PathlingContext` JVM
object, which is stored as a `spark_jobj`.

Existing R functions that return columns (e.g. `tx_member_of`) use
`rlang::expr()` to create quoted expressions for use within dplyr/sparklyr
verbs. However, `searchToColumn` returns a JVM `Column` object that cannot be
expressed as a simple SQL expression — it must be invoked via Java interop.

## Goals / Non-goals

**Goals:**

- Expose `PathlingContext.searchToColumn` to R users as `pc_search_to_column`.
- Return a value usable with sparklyr DataFrame operations (`sdf_filter`,
  `filter`, `select`).
- Follow existing R library conventions for naming, documentation, and testing.

**Non-goals:**

- Implementing `fhirpath_to_column` (no Java API method exists yet on
  `PathlingContext`).
- Supporting column combination operators in R (AND/OR) — Spark column JVM
  objects already support these via `j_invoke` if needed, but that is outside
  this scope.

## Decisions

### Function signature

Use `pc_search_to_column(pc, resource_type, search_expression)` following the
`pc_` prefix convention used by other context-bound operations in the R library
(e.g. `pathling_connect`, `pathling_spark`).

**Rationale**: The `pc_` prefix clearly signals that the function operates on a
PathlingContext object. The parameter order matches both the Java and Python
APIs.

**Alternative considered**: Piped style `pc %>% search_to_column(...)` — this
works naturally since the `pc` is the first argument regardless of prefix.

### Return type

Return the raw `spark_jobj` (JVM Column reference) directly from `j_invoke`.
This is consistent with how sparklyr handles Column objects internally and
allows the result to be used with `sdf_filter` and other sparklyr operations
that accept JVM Column references.

**Rationale**: sparklyr's `filter` and `sdf_filter` already accept `spark_jobj`
Column objects. No wrapping is needed.

### File placement

Add the function to `lib/R/R/context.R` alongside other PathlingContext
operations.

## Risks / Trade-offs

- **JVM Column objects are opaque in R**: Unlike Python's PySpark `Column`
  class, the R `spark_jobj` has no operator overloads (`&`, `|`). Users who
  need to combine columns must use `j_invoke` directly. → This is acceptable
  for the initial implementation; a higher-level wrapper could be added later.
