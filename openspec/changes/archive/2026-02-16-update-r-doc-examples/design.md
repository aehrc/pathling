## Context

The R library provides `pathling_search_to_column` and
`pathling_fhirpath_to_column`, which return raw Spark Column objects. Filtering
a DataFrame with these requires users to call
`sparklyr::j_invoke("filter", col)` — an internal Spark Java interop detail.

The `pathling_filter` helper already wraps this pattern, accepting a DataFrame
as its first argument so it works with the pipe operator.

## Goals / Non-goals

**Goals:**

- All R examples in roxygen documentation and site docs use `pathling_filter`
  for filtering, with no `j_invoke` visible to users.
- Retain non-filtering examples (e.g. value extraction with
  `pathling_fhirpath_to_column`) where `pathling_filter` is not applicable.

**Non-goals:**

- Changing the implementation of any R functions.
- Removing `pathling_search_to_column` or `pathling_fhirpath_to_column` from
  the API.
- Updating R test code — tests may legitimately use lower-level APIs.

## Decisions

1. **Replace `j_invoke` filter examples with `pathling_filter`** — the helper
   already exists and is pipe-friendly. This is the simplest path.
2. **Keep `pathling_search_to_column` / `pathling_fhirpath_to_column` examples
   that demonstrate column creation** — these functions are still part of the
   public API; examples should show what they return, but filtering should use
   the helper.
3. **Site docs show only the `pathling_filter` approach** — remove the
   side-by-side "alternatively" pattern and use `pathling_filter` as the primary
   (and only) example.

## Risks / Trade-offs

- Users reading old cached docs may still see `j_invoke` — acceptable, as the
  functions still work.
