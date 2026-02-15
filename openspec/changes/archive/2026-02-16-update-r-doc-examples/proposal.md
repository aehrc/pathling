## Why

R documentation examples for `pathling_search_to_column` and
`pathling_fhirpath_to_column` expose internal Spark Java interop (`j_invoke`)
to users. The `pathling_filter` helper already encapsulates this pattern, so
examples should use it instead, giving users a cleaner, more idiomatic API
surface.

## What changes

- Replace `j_invoke("filter", ...)` patterns in roxygen examples for
  `pathling_search_to_column` and `pathling_fhirpath_to_column` with
  `pathling_filter`.
- Update site documentation (`search.md`, `fhirpath.md`) to use
  `pathling_filter` as the primary example, removing the `j_invoke` variant.
- Regenerate `.Rd` man pages from updated roxygen comments.

## Capabilities

### New capabilities

None.

### Modified capabilities

- `fhirpath-to-column-r`: Examples updated to use `pathling_filter` instead of
  `j_invoke`.
- `r-search-to-column`: Examples updated to use `pathling_filter` instead of
  `j_invoke`.

## Impact

- `lib/R/R/context.R` — roxygen `@examples` blocks for
  `pathling_search_to_column` and `pathling_fhirpath_to_column`.
- `lib/R/man/pathling_search_to_column.Rd` and
  `lib/R/man/pathling_fhirpath_to_column.Rd` — regenerated from roxygen.
- `site/docs/libraries/search.md` and `site/docs/libraries/fhirpath.md` — R
  code blocks.
