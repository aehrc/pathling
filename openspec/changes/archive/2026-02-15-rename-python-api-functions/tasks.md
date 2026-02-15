## 1. Rename core functions

- [x] 1.1 Rename `pc_fhirpath_to_column` to `pathling_fhirpath_to_column` in lib/R/R/context.R
- [x] 1.2 Rename `pc_search_to_column` to `pathling_search_to_column` in lib/R/R/context.R
- [x] 1.3 Update roxygen2 documentation for `pathling_fhirpath_to_column` to use new name in examples
- [x] 1.4 Update roxygen2 documentation for `pathling_search_to_column` to use new name in examples

## 2. Update internal usages

- [x] 2.1 Update `pathling_filter` function to call `pathling_search_to_column` instead of `pc_search_to_column`
- [x] 2.2 Update `pathling_filter` function to call `pathling_fhirpath_to_column` instead of `pc_fhirpath_to_column`
- [x] 2.3 Update `pathling_with_column` function to call `pathling_fhirpath_to_column` instead of `pc_fhirpath_to_column`

## 3. Update tests

- [x] 3.1 Update all test calls to `pc_search_to_column` in lib/R/tests/testthat/test-context.R to use `pathling_search_to_column`
- [x] 3.2 Update all test calls to `pc_fhirpath_to_column` in lib/R/tests/testthat/test-context.R to use `pathling_fhirpath_to_column`
- [x] 3.3 Update test section headers from "pc_fhirpath_to_column tests" to "pathling_fhirpath_to_column tests"
- [x] 3.4 Update test section headers from "pc_search_to_column tests" to "pathling_search_to_column tests"

## 4. Update site documentation

- [x] 4.1 Update all references to `pc_fhirpath_to_column` in site/docs/libraries/fhirpath.md to use `pathling_fhirpath_to_column`
- [x] 4.2 Update all references to `pc_search_to_column` in site/docs/libraries/search.md to use `pathling_search_to_column`

## 5. Verify implementation

- [x] 5.1 Run all R tests to verify functions work with new names
- [x] 5.2 Verify roxygen2 documentation generates correctly with new function names
- [x] 5.3 Verify NAMESPACE file exports the new function names
