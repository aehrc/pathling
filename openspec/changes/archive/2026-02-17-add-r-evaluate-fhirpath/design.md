## Context

The Python library already wraps `PathlingContext.evaluateFhirPath()` via Py4J,
converting the Java `SingleInstanceEvaluationResult` into a Python dictionary.
The R library uses sparklyr's `j_invoke` for all JVM interop and currently has
no equivalent function. The Java method accepts `(String, String, String,
String, Map)` and returns a `SingleInstanceEvaluationResult` containing a
`List<TypedValue>` and a `String`.

## Goals / Non-Goals

**Goals:**

- Provide `pathling_evaluate_fhirpath()` in R with the same capabilities as the
  Python `evaluate_fhirpath()`.
- Document single-resource FHIRPath evaluation in `fhirpath.md` across all four
  languages.

**Non-Goals:**

- Changing the Java API or `SingleInstanceEvaluationResult` class.
- Adding AST/parse tree information to the R result (the Java API does not
  expose this through the `evaluateFhirPath` method).
- Supporting streaming or batch evaluation of multiple resources.

## Decisions

### Call the 5-argument Java overload directly

The Java `evaluateFhirPath` has two overloads: a 3-argument (expression only)
and a 5-argument (with optional context expression and variables). The R
function will always call the 5-argument overload, passing `NULL` for the
optional parameters when they are not provided. This mirrors the Python
implementation and avoids method-resolution complexity in sparklyr.

### Convert Java result to an R list

The function will iterate over `jresult.getResults()` using `j_invoke`, extract
each `TypedValue`'s type and value, and build an R list with the structure:

```r
list(
  results = list(
    list(type = "string", value = "Smith"),
    list(type = "string", value = "Jones")
  ),
  expectedReturnType = "string"
)
```

This mirrors the Python dictionary structure and is idiomatic R.

### Convert Java variable map via HashMap

When the user passes R named variables, the function will create a
`java.util.HashMap` via `j_invoke_static` / `j_invoke` and populate it with the
named entries. This matches how the Python library passes its `dict` to Py4J.

### Documentation section placement

The new "Single resource evaluation" section will be placed at the end of the
existing `fhirpath.md` page, after "Combining with other operations". This is a
distinct use case from DataFrame-based operations and logically comes last.

## Risks / Trade-offs

- **Java type coercion**: sparklyr's `j_invoke` handles basic Java-to-R type
  conversions (String, Integer, Boolean, Double). Complex FHIR types are
  returned as JSON strings by the Java API, so no special handling is needed.
  Risk is low.
- **NULL handling**: Java `null` values from `TypedValue.getValue()` will map to
  R `NULL` via sparklyr. This is consistent with R conventions.
