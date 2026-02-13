## 1. Tests

- [x] 1.1 Add unit tests for `pathling_filter` with FHIRPath expressions to `lib/R/tests/testthat/test-context.R`
- [x] 1.2 Add unit tests for `pathling_filter` with search expressions (`type = "search"`)
- [x] 1.3 Add unit tests for `pathling_with_column` (single column, chained columns, error case)
- [x] 1.4 Add unit test for piped combination of `pathling_filter` and `pathling_with_column`

## 2. Implementation

- [x] 2.1 Implement `pathling_filter` in `lib/R/R/context.R`
- [x] 2.2 Implement `pathling_with_column` in `lib/R/R/context.R`
- [x] 2.3 Add roxygen2 documentation with examples for both functions

## 3. Manual test script

- [x] 3.1 Update `~/Code/pathling-scripts/test_fhirpath_to_column.R` to use the new functions

## 4. Verification

- [x] 4.1 Run R unit tests and confirm they pass
- [x] 4.2 Run the manual test script and confirm all checks pass
