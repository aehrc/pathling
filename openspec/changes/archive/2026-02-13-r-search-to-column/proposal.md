## Why

The Java library API and Python library already expose `search_to_column`, but
the R library has no equivalent. R users cannot convert FHIR search expressions
into Spark columns for use in DataFrame operations.

## What changes

- Add `pc_search_to_column(pc, resource_type, search_expression)` to the R
  library, wrapping `PathlingContext.searchToColumn`.
- Add corresponding roxygen2 documentation and unit tests.

## Capabilities

### New capabilities

- `r-search-to-column`: R function that converts a FHIR search query string
  into a Spark column via the Java library API.

### Modified capabilities

(none)

## Impact

- **Code**: `lib/R/R/context.R` (new exported function), `lib/R/NAMESPACE`,
  `lib/R/man/` (generated docs).
- **Tests**: New test file or additions to `lib/R/tests/testthat/test-context.R`.
- **Dependencies**: No new dependencies; uses existing sparklyr Java
  interop (`j_invoke`).
