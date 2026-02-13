## 1. Tests

- [x] 1.1 Write tests that verify encoding/decoding round-trip behaviour is
      preserved after the refactor (covering representative FHIR resource types)
- [x] 1.2 Run tests and confirm they pass with the current (pre-refactor)
      implementation

## 2. Implementation

- [x] 2.1 Replace `FhirAgnosticEncoder` to extend `AgnosticEncoder` instead of
      `AgnosticExpressionPathEncoder`, removing the `toCatalyst`/`fromCatalyst`
      methods
- [x] 2.2 Change `EncoderBuilder.of` to construct `ExpressionEncoder` directly
      using the case class constructor with the pre-built serialiser and
      deserialiser expressions
- [x] 2.3 Remove the `@annotation.nowarn("cat=deprecation")` annotation and the
      import of `AgnosticExpressionPathEncoder`
- [x] 2.4 Update comments to reflect the new approach

## 3. Verification

- [x] 3.1 Run tests from step 1 and confirm they still pass
- [x] 3.2 Run the full encoders module test suite to check for regressions
