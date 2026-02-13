## Why

The Pathling library API exposes `searchToColumn` for converting FHIR search expressions to Spark columns, but there is no equivalent for arbitrary FHIRPath expressions. Exposing FHIRPath evaluation as a general-purpose column-producing method would allow users to leverage the full expressiveness of FHIRPath directly within Spark DataFrame operations such as `filter()`, `where()`, and `select()`.

## What changes

- Add `fhirPathToColumn(String resourceType, String fhirPathExpression)` method to `PathlingContext` in the Java library API.
- Add `fhirpath_to_column(resource_type, fhirpath_expression)` to the Python library bindings.
- Add `pc_fhirpath_to_column(pc, resource_type, fhirpath_expression)` to the R library bindings.
- Add documentation and tests for all three language bindings.

## Capabilities

### New capabilities

- `fhirpath-to-column-java`: Java library API method that converts a FHIRPath expression into a Spark Column using the existing FHIRPath engine and `SearchColumnBuilder.fromExpression()`.
- `fhirpath-to-column-python`: Python binding that exposes the Java method via py4j, returning a PySpark Column.
- `fhirpath-to-column-r`: R binding that exposes the Java method via sparklyr's `j_invoke`, returning a spark_jobj Column.

### Modified capabilities

(none)

## Impact

- **library-api**: New public method on `PathlingContext`.
- **lib/python**: New function on `PathlingContext` class.
- **lib/R**: New exported function in `context.R`.
- **Tests**: New unit tests in Java, Python, and R.
- **Documentation**: Updates to site documentation for all three languages.
