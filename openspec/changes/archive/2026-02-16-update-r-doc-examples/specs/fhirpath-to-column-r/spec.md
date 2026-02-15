## MODIFIED Requirements

### Requirement: pathling_fhirpath_to_column has roxygen2 documentation

The function SHALL include roxygen2 documentation with a title, description, parameter documentation, return value, and export tag. The `@examples` block SHALL demonstrate filtering using `pathling_filter` instead of raw `j_invoke` calls.

#### Scenario: Documentation is present

- **WHEN** the function is defined
- **THEN** it SHALL have roxygen2 comments documenting its purpose, parameters (`pc`, `resource_type`, `fhirpath_expression`), and return value

#### Scenario: Examples use pathling_filter for filtering

- **WHEN** the roxygen `@examples` block demonstrates filtering a DataFrame
- **THEN** it SHALL use `pathling_filter` and SHALL NOT contain `j_invoke`
