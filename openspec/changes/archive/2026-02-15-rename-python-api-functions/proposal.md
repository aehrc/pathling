## Why

The R API functions `pc_fhirpath_to_column` and `pc_search_to_column` use the `pc_` prefix, which is not descriptive. Renaming to `pathling_` makes the API more discoverable and aligns with the product name and existing naming conventions used by other R functions like `pathling_connect`, `pathling_filter`, and `pathling_with_column`.

## What Changes

- **BREAKING**: Rename `pc_fhirpath_to_column` to `pathling_fhirpath_to_column` in the R API
- **BREAKING**: Rename `pc_search_to_column` to `pathling_search_to_column` in the R API
- Update internal usages of these functions in `pathling_filter` and `pathling_with_column`
- Update all tests to use the new function names
- Update documentation and examples to reference the new names

## Capabilities

### New Capabilities

<!-- None -->

### Modified Capabilities

- `fhirpath-to-column-r`: Rename the function from `pc_fhirpath_to_column` to `pathling_fhirpath_to_column`
- `r-search-to-column`: Rename the function from `pc_search_to_column` to `pathling_search_to_column`

## Impact

- R users calling `pc_fhirpath_to_column` or `pc_search_to_column` will need to update their code
- Functions `pathling_filter` and `pathling_with_column` that internally use these functions will be updated
- All tests and documentation will be updated to use the new names
- Site documentation (site/docs/libraries/fhirpath.md and site/docs/libraries/search.md) will be updated
- No impact on Java, Python, or other language bindings
