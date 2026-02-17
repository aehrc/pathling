## Context

When `PathlingContext.evaluate_fhirpath()` returns complex types (e.g.
Quantity), the result is serialised to JSON via `Row.json()`. This includes all
columns from the Spark Row, including internal metadata columns added by the
encoders for efficient querying:

- `value_scale` &mdash; BigDecimal scale storage for Quantity values.
- `_value_canonicalized` &mdash; UCUM-normalised value for cross-unit comparison.
- `_code_canonicalized` &mdash; UCUM-normalised code for cross-unit comparison.
- `_fid` &mdash; internal field ID for extension support.

These columns are implementation details and should not appear in user-facing
output.

Additionally, `PathlingContext` has grown to contain ~250 lines of FHIRPath
evaluation logic (parsing, evaluation, result collection, value materialisation,
JSON conversion). This logic belongs in the `fhirpath` module, not in the
library API facade.

The `variables` parameter on the Java `evaluateFhirPath` is accepted but never
wired into evaluation. FHIRPath Lab sends custom `%varName` variables, and the
`SingleResourceEvaluator` already supports them via
`.withVariables(Map<String, Collection>)`. The missing piece is converting the
incoming variable values (primitives, JSON) into `Collection` objects and
passing them through.

## Goals / Non-goals

**Goals:**

- Strip internal metadata columns from complex type JSON output in the
  `evaluate_fhirpath` API so all consumers (including FHIRPath Lab) receive
  clean FHIR-conformant results.
- Keep the definition of "synthetic field" in a single place so that
  `PruneSyntheticFields` and the new sanitisation logic cannot diverge.
- Extract all FHIRPath evaluation logic from `PathlingContext` into a dedicated
  class in the `fhirpath` module to improve cohesion.
- Wire up the `variables` parameter so custom FHIRPath variables are resolved
  during evaluation.

**Non-goals:**

- Changing the internal Spark schema or how metadata columns are stored.
- Sanitising output from other APIs (e.g. ViewDefinition, extract) &mdash; these
  have their own output pipelines.

## Decisions

### Extract synthetic field predicate into a shared Java utility

Create a static `isSyntheticField(String name)` method in a Java utility class
within the `encoders` module (e.g. `SyntheticFieldUtils` in the
`au.csiro.pathling.sql` package, alongside `PruneSyntheticFields`). This method
returns `true` if the name starts with `_` or ends with `_scale`.

Refactor `PruneSyntheticFields.isAnnotation` to delegate to this utility. This
ensures a single source of truth for what constitutes a synthetic field.

### Extract all evaluation logic into `SingleInstanceEvaluator`

Create `SingleInstanceEvaluator` in the `fhirpath` module that owns the entire
evaluation lifecycle:

- Encoding a single resource JSON into a one-row `Dataset<Row>` (uses
  `PathlingContext` for access to `encode()` and `SparkSession`).
- Expression parsing and evaluation.
- Context expression handling.
- Variable conversion and wiring.
- Return type determination.
- Result collection, value materialisation, and JSON conversion.
- Row sanitisation (stripping synthetic fields).

`PathlingContext` retains two thin overloads that delegate to
`SingleInstanceEvaluator`:

```java
public FhirPathResult evaluateFhirPath(
    String resourceType, String resourceJson, String fhirPathExpression)

public FhirPathResult evaluateFhirPath(
    String resourceType, String resourceJson,
    String fhirPathExpression, String contextExpression,
    Map<String, Object> variables)
```

`FhirPathResult` and `TypedValue` move to the `fhirpath` module.

### Wire up variables

The `SingleResourceEvaluatorBuilder` already supports
`.withVariables(Map<String, Collection>)`. The new `SingleInstanceEvaluator`
converts the incoming `Map<String, Object>` variable values into `Collection`
objects:

- Primitive values (String, Integer, Decimal, Boolean) are converted to
  literal `Collection` instances using the appropriate FHIRPath type.
- The converted map is passed to
  `SingleResourceEvaluatorBuilder.withVariables()`.

FHIRPath Lab sends variables as typed FHIR Parameters parts (e.g.
`valueString`, `valueInteger`, `valueBoolean`, `valueDecimal`). The Python
layer already extracts these as typed values and passes them through.

### Apply row sanitisation in `SingleInstanceEvaluator`

`SingleInstanceEvaluator` applies `sanitiseRow` (using
`SyntheticFieldUtils.isSyntheticField`) in `rowToJson()` before calling
`row.json()`. This constructs a new `GenericRowWithSchema` with synthetic
fields removed, applied recursively to handle nested struct types.

## Risks / Trade-offs

- **Trade-off**: Minor runtime cost of constructing a filtered Row for each
  complex type result. Acceptable given `evaluate_fhirpath` processes
  individual resources, not bulk data.
- **Risk**: Moving `FhirPathResult` to the `fhirpath` module changes import
  paths for any Java consumers. **Mitigation**: `library-api` is the public
  API surface; can re-export if needed.
