## 1. Tests for the bug fix

- [x] 1.1 Add `name` and `address` parameters to `TestSearchParameterRegistry`
      (STRING type, pointing to `Patient.name` and `Patient.address`)
- [x] 1.2 Add unit tests to `SearchColumnBuilderTest` for string search on
      HumanName fields (name parameter matching family, given, text, prefix, suffix)
- [x] 1.3 Add unit tests to `SearchColumnBuilderTest` for string search on
      Address fields (address parameter matching all sub-fields)
- [x] 1.4 Add unit tests for exact string modifier on complex types
      (`name:exact=Smith`)

## 2. Implementation

- [x] 2.1 Add static sub-field mappings for HumanName and Address to
      `SearchColumnBuilder`
- [x] 2.2 Modify `buildExpressionFilter()` to detect complex types and expand
      into sub-field expressions, OR-ing the filter results together

## 3. Expand test coverage for other parameter types

Add data-level tests to `SearchColumnBuilderTest` for combinations that
currently have zero data-level coverage.

- [x] 3.1 STRING + string: `family=smith` matching Patient.name.family
- [x] 3.2 TOKEN + CodeableConcept: `code=...` matching Observation.code
- [x] 3.3 TOKEN + Identifier: `identifier=...` matching Patient.identifier
- [x] 3.4 TOKEN + ContactPoint: `telecom=...` matching Patient.telecom
- [x] 3.5 DATE + date: `birthdate=...` matching Patient.birthDate
- [x] 3.6 DATE + Period: `period=...` matching Coverage.period
- [x] 3.7 DATE + instant: `date=...` matching AuditEvent.recorded
- [x] 3.8 NUMBER + decimal: `probability=...` matching
      RiskAssessment.prediction.probability
- [x] 3.9 QUANTITY + Quantity: `value-quantity=...` matching
      Observation.value.ofType(Quantity)

## 4. Verification

- [x] 4.1 Run all `SearchColumnBuilderTest` tests to confirm no regressions
- [x] 4.2 Run server-level `SearchExecutorTest` or integration tests that
      exercise the search endpoint
