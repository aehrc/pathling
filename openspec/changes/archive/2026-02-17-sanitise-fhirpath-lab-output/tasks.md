## 1. Shared utility

- [x] 1.1 Create `SyntheticFieldUtils` in `encoders` module
      (`au.csiro.pathling.sql` package) with a static `isSyntheticField(String)`
      method that returns true for names starting with `_` or ending with `_scale`
- [x] 1.2 Write unit tests for `SyntheticFieldUtils.isSyntheticField`
- [x] 1.3 Refactor `PruneSyntheticFields.isAnnotation` to delegate to
      `SyntheticFieldUtils.isSyntheticField`

## 2. Extract evaluation logic

- [x] 2.1 Create `SingleInstanceEvaluator` in the `fhirpath` module that owns
      the full evaluation lifecycle: resource encoding into a one-row Dataset,
      expression parsing, evaluation, context/variables handling, return type
      determination, result collection, value materialisation, and JSON conversion
- [x] 2.2 Move `FhirPathResult` and `TypedValue` to the `fhirpath` module
- [x] 2.3 Slim `PathlingContext` to two thin `evaluateFhirPath` overloads
      (3-arg and 5-arg) that delegate to `SingleInstanceEvaluator`; remove all
      private evaluation helpers

## 3. Wire up variables

- [x] 3.1 In `SingleInstanceEvaluator`, convert incoming `Map<String, Object>`
      variable values into `Map<String, Collection>` (String, Integer, Decimal,
      Boolean to literal Collections) and pass them to
      `SingleResourceEvaluatorBuilder.withVariables()`
- [x] 3.2 Write tests for variable conversion and end-to-end variable
      resolution (string, integer, boolean, decimal variables referenced via
      `%varName` in expressions)

## 4. Row sanitisation

- [x] 4.1 Write unit tests for a `sanitiseRow` method that strips synthetic
      fields from a Spark Row, including recursive handling of nested structs
- [x] 4.2 Add `sanitiseRow` to `SingleInstanceEvaluator`, using
      `SyntheticFieldUtils.isSyntheticField`, applied recursively to nested struct
      fields
- [x] 4.3 Call `sanitiseRow` from `rowToJson()` before `row.json()`
- [x] 4.4 Write an integration test for `evaluate_fhirpath` that evaluates a
      Quantity expression and asserts the JSON result does not contain
      `value_scale`, `_value_canonicalized`, or `_code_canonicalized`

## 5. Verification

- [x] 5.1 Run all tests and verify they pass
