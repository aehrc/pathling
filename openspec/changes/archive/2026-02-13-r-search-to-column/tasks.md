## 1. Tests

- [x] 1.1 Write tests for `pc_search_to_column` in `lib/R/tests/testthat/test-context.R` covering single parameter, multiple parameters, date prefix, empty expression, DataFrame filtering, and invalid parameter scenarios.
- [x] 1.2 Run tests and verify they fail (no implementation yet).

## 2. Implementation

- [x] 2.1 Add `pc_search_to_column(pc, resource_type, search_expression)` function to `lib/R/R/context.R` with roxygen2 documentation.
- [x] 2.2 Add the `@export` tag and regenerate NAMESPACE via `devtools::document()`.
- [x] 2.3 Run tests and verify they pass.

## 3. Verification

- [x] 3.1 Run `R CMD check` or the package build to confirm no warnings or errors.
