## Why

The FHIRPath Lab API returns synthetic fields (`value_scale`,
`_value_canonicalized`, `_code_canonicalized`) in complex type results like
Quantity. These are Spark encoding implementation details used for efficient
comparisons and should not be exposed to users.

Additionally, `PathlingContext` has accumulated FHIRPath evaluation logic that
belongs in the `fhirpath` module. Extracting it improves cohesion and makes the
evaluation logic reusable independently of the library API context.

The `variables` parameter on the Java `evaluateFhirPath` overload is accepted
but never wired into evaluation. FHIRPath Lab sends custom `%varName`
variables, and the `SingleResourceEvaluator` already supports them via
`.withVariables(Map<String, Collection>)`. The missing piece is converting
incoming variable values into `Collection` objects.

## What changes

- Extract the synthetic field predicate from `PruneSyntheticFields.isAnnotation`
  into a shared Java utility (`SyntheticFieldUtils`) so the definition is
  reusable from both Scala and Java code.
- Extract all FHIRPath evaluation logic from `PathlingContext` into
  `SingleInstanceEvaluator` in the `fhirpath` module. This includes resource
  encoding, expression parsing, evaluation, context/variables handling, result
  collection, value materialisation, and JSON conversion.
- `PathlingContext` retains two thin `evaluateFhirPath` overloads (3-arg and
  4-arg with context) that delegate to `SingleInstanceEvaluator`.
- Wire up the `variables` parameter properly: convert incoming variable values
  into `Collection` objects and pass them to
  `SingleResourceEvaluatorBuilder.withVariables()`.
- Sanitise complex type results in `SingleInstanceEvaluator` using the shared
  predicate, stripping synthetic fields before JSON serialisation.

## Capabilities

### New capabilities

_None._

### Modified capabilities

- `fhirpath-lab-server`: Strip synthetic fields from complex type results
  before returning them in the API response.
- `fhirpath-evaluation-api`: Wire up `variables` parameter so that custom
  FHIRPath variables (`%varName`) are resolved during evaluation.

## Impact

- `encoders` module: new `SyntheticFieldUtils` class; `PruneSyntheticFields`
  refactored to delegate to it.
- `fhirpath` module: new `SingleInstanceEvaluator` encapsulating all evaluation
  and result materialisation logic, including variable conversion and row
  sanitisation. `FhirPathResult` and `TypedValue` move here.
- `library-api` module: `PathlingContext` slimmed down to two thin
  `evaluateFhirPath` delegate methods.
