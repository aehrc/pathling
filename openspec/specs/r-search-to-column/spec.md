## ADDED Requirements

### Requirement: Convert FHIR search expression to Spark column

The R library SHALL provide a function `pathling_search_to_column(pc, resource_type,
search_expression)` that accepts a PathlingContext, a FHIR resource type name,
and a FHIR search query string, and returns a Spark Column object representing
the boolean filter condition.

#### Scenario: Single search parameter

- **WHEN** `pathling_search_to_column(pc, "Patient", "gender=male")` is called
- **THEN** a Spark Column object (`spark_jobj`) is returned that evaluates to
  true for Patient resources where gender equals male.

#### Scenario: Multiple search parameters (AND)

- **WHEN** `pathling_search_to_column(pc, "Patient", "gender=male&active=true")` is
  called
- **THEN** a Spark Column object is returned that evaluates to true for Patient
  resources matching both conditions.

#### Scenario: Date prefix search

- **WHEN** `pathling_search_to_column(pc, "Patient", "birthdate=ge1990-01-01")` is
  called
- **THEN** a Spark Column object is returned that evaluates to true for Patient
  resources with birth date on or after 1990-01-01.

#### Scenario: Empty search expression

- **WHEN** `pathling_search_to_column(pc, "Patient", "")` is called
- **THEN** a Spark Column object is returned that evaluates to true for all
  Patient resources.

#### Scenario: Filter a DataFrame

- **WHEN** a Column returned by `pathling_search_to_column` is applied to a DataFrame
  of Patient resources using sparklyr's filter
- **THEN** only resources matching the search criteria are retained.

#### Scenario: Invalid search parameter

- **WHEN** `pathling_search_to_column(pc, "Patient", "invalid-param=value")` is
  called
- **THEN** an error is raised indicating the parameter is unknown.

### Requirement: Function is exported and documented

The `pathling_search_to_column` function SHALL be exported in the package NAMESPACE
and SHALL have roxygen2 documentation including description, parameter
definitions, return value, and usage examples. The `@examples` block SHALL demonstrate filtering using `pathling_filter` instead of raw `j_invoke` calls.

#### Scenario: Function is accessible after loading the package

- **WHEN** the pathling package is loaded with `library(pathling)`
- **THEN** `pathling_search_to_column` is available without qualifying the namespace.

#### Scenario: Documentation is generated

- **WHEN** `devtools::document()` is run on the package
- **THEN** a man page is generated for `pathling_search_to_column` with all
  documented parameters and examples.

#### Scenario: Examples use pathling_filter for filtering

- **WHEN** the roxygen `@examples` block demonstrates filtering a DataFrame
- **THEN** it SHALL use `pathling_filter` and SHALL NOT contain `j_invoke`
