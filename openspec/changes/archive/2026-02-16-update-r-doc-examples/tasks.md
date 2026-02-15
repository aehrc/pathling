## 1. Roxygen documentation

- [x] 1.1 Update `@examples` in `pathling_search_to_column` (`lib/R/R/context.R` ~line 296) to use `pathling_filter` instead of `j_invoke`
- [x] 1.2 Update `@examples` in `pathling_fhirpath_to_column` (`lib/R/R/context.R` ~line 338) to use `pathling_filter` instead of `j_invoke`
- [x] 1.3 Regenerate `.Rd` man pages by running `devtools::document()` on the R package

## 2. Site documentation

- [x] 2.1 Update R example in `site/docs/libraries/search.md` (~line 40) to use only `pathling_filter` and remove the `j_invoke` variant
- [x] 2.2 Update R example in `site/docs/libraries/fhirpath.md` (~line 40) to use only `pathling_filter` and remove the `j_invoke` variant
- [x] 2.3 Update R example in `site/docs/libraries/search.md` (~line 1160) to use only `pathling_filter` and remove the `j_invoke` variant

## 3. Verification

- [x] 3.1 Grep the codebase to confirm no `j_invoke` remains in any R example blocks (roxygen or site docs)
