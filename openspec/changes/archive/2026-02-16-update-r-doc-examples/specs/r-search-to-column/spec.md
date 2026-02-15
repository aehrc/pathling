## MODIFIED Requirements

### Requirement: Function is exported and documented

The `pathling_search_to_column` function SHALL be exported in the package NAMESPACE and SHALL have roxygen2 documentation including description, parameter definitions, return value, and usage examples. The `@examples` block SHALL demonstrate filtering using `pathling_filter` instead of raw `j_invoke` calls.

#### Scenario: Function is accessible after loading the package

- **WHEN** the pathling package is loaded with `library(pathling)`
- **THEN** `pathling_search_to_column` is available without qualifying the namespace.

#### Scenario: Documentation is generated

- **WHEN** `devtools::document()` is run on the package
- **THEN** a man page is generated for `pathling_search_to_column` with all documented parameters and examples.

#### Scenario: Examples use pathling_filter for filtering

- **WHEN** the roxygen `@examples` block demonstrates filtering a DataFrame
- **THEN** it SHALL use `pathling_filter` and SHALL NOT contain `j_invoke`
