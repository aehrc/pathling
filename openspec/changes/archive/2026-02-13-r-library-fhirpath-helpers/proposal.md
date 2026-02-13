## Why

The R library's `pc_fhirpath_to_column` and `pc_search_to_column` functions return raw JVM Column objects (`spark_jobj`). Using these requires low-level `j_invoke("filter", ...)` and `j_invoke("withColumn", ...)` calls with manual `sdf_register()`, which is not idiomatic sparklyr. Spark's `Dataset.select(Column*)` is particularly problematic because Scala varargs cannot be called from sparklyr's `j_invoke`. Users should be able to pipe FHIRPath and search results into standard dplyr workflows.

## What changes

- Add `pathling_filter()` — filters a `tbl_spark` using a FHIRPath boolean expression or FHIR search query, returning a `tbl_spark`.
- Add `pathling_with_column()` — adds a named column derived from a FHIRPath expression to a `tbl_spark`, returning a `tbl_spark`.
- Both follow the sparklyr `sdf_with_*` convention: DataFrame as first argument, pipeable, return a `tbl_spark`.
- The existing low-level `pc_fhirpath_to_column` and `pc_search_to_column` remain unchanged for advanced use.

## Capabilities

### New capabilities

- `r-fhirpath-helpers`: DataFrame-level convenience functions (`pathling_filter`, `pathling_with_column`) that wrap JVM Column objects in idiomatic sparklyr API.

### Modified capabilities

_(none)_

## Impact

- `lib/R/R/context.R` — new exported functions added.
- `lib/R/tests/testthat/test-context.R` — new tests for the convenience functions.
- Existing `pc_fhirpath_to_column` and `pc_search_to_column` are unaffected.
- Test script `~/Code/pathling-scripts/test_fhirpath_to_column.R` updated to use the new functions.
