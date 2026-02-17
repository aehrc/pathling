## Why

The Python library provides `evaluate_fhirpath()` for evaluating a FHIRPath
expression against a single FHIR resource (as a JSON string) and returning
materialised typed results. The R library has no equivalent, limiting R users to
DataFrame-based operations only. Additionally, the library documentation lacks a
section showing how to evaluate FHIRPath against a single resource.

## What Changes

- Add a `pathling_evaluate_fhirpath()` function to the R library that calls
  `PathlingContext.evaluateFhirPath()` via sparklyr's `j_invoke`, converts the
  Java result to an R list, and returns it.
- Add a "Single resource evaluation" section to
  `site/docs/libraries/fhirpath.md` with examples in all four languages (Python,
  R, Scala, Java).

## Capabilities

### New Capabilities

- `r-evaluate-fhirpath`: R binding for evaluating a FHIRPath expression against
  a single FHIR resource and returning typed results.

### Modified Capabilities

_(none)_

## Impact

- `lib/R/R/context.R` - new exported function and roxygen2 documentation.
- `lib/R/NAMESPACE` - updated by roxygen2 to export the new function.
- `lib/R/tests/testthat/test-context.R` - new tests for the function.
- `site/docs/libraries/fhirpath.md` - new documentation section.
