## 1. Register the union operator

- [x] 1.1 Add `UNION("|", new UnionOperator())` to the `BinaryOperatorType` enum in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/BinaryOperatorType.java`

## 2. Verify

- [x] 2.1 Run `CombiningOperatorsDslTest` and confirm all 19 tests execute and pass (not skipped)
- [x] 2.2 Run the full fhirpath module test suite to check for regressions
