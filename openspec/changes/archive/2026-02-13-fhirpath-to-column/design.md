## Context

The Pathling library API (`PathlingContext`) already provides methods for evaluating FHIRPath expressions in contexts like `extract()` and `aggregate()`, and recently added `searchToColumn()` for converting FHIR search expressions to Spark columns. The infrastructure for evaluating a standalone FHIRPath expression and returning a Spark `Column` already exists in `SearchColumnBuilder.fromExpression()`. This change surfaces that capability as a public API method.

## Goals / non-goals

**Goals:**

- Expose `fhirPathToColumn` as a first-class method on `PathlingContext` in Java, Python, and R.
- Follow the same patterns as `searchToColumn` for consistency across all three language bindings.
- Enable users to combine FHIRPath-derived columns with standard Spark DataFrame operations.

**Non-goals:**

- Changing FHIRPath evaluation semantics or adding new FHIRPath functions.
- Supporting multi-valued (collection) results — the method returns a single column value.
- Performance optimisation of the underlying FHIRPath engine.

## Decisions

### Delegate to `SearchColumnBuilder.fromExpression()`

The `SearchColumnBuilder` already implements `fromExpression(ResourceType, String)` which parses and evaluates a FHIRPath expression against a flat resource schema, returning a Spark `Column`. The new `PathlingContext.fhirPathToColumn()` method will delegate to this directly, mirroring how `searchToColumn()` delegates to `SearchColumnBuilder.fromQueryString()`.

**Alternative considered:** Building a separate evaluation pipeline from `Parser` and `SingleResourceEvaluator`. Rejected because `SearchColumnBuilder` already encapsulates this logic and handles the flat schema evaluator setup.

### Language binding pattern

Python and R bindings will follow the exact same pattern as `searchToColumn`:

- **Python:** Call `self._jpc.fhirPathToColumn(resource_type, fhirpath_expression)` via py4j, wrap result in PySpark `Column`.
- **R:** Call `j_invoke(pc, "fhirPathToColumn", ...)` via sparklyr, return raw `spark_jobj`.

### Method naming

- Java: `fhirPathToColumn` (camelCase, consistent with `searchToColumn`)
- Python: `fhirpath_to_column` (snake_case, PEP 8)
- R: `pc_fhirpath_to_column` (snake*case with `pc*` prefix, matching existing R API conventions)

## Risks / trade-offs

- **FHIRPath expressions producing collections**: If a FHIRPath expression returns a collection rather than a scalar, `Collection.getColumn().getValue()` may produce unexpected results. Mitigation: document that the expression should evaluate to a single value per resource row.
- **Error handling**: Invalid FHIRPath expressions will throw exceptions from the parser. Mitigation: let these propagate naturally — consistent with `searchToColumn` behaviour.
