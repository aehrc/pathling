## ADDED Requirements

### Requirement: R library exposes pathling_fhirpath_to_column function

The R library SHALL provide an exported function `pathling_fhirpath_to_column(pc, resource_type, fhirpath_expression)` that invokes the Java `PathlingContext.fhirPathToColumn()` method via sparklyr's `j_invoke` and returns a `spark_jobj` representing a Spark Column.

#### Scenario: Boolean expression returns a Column for filtering

- **WHEN** `pc %>% pathling_fhirpath_to_column("Patient", "gender = 'male'")` is called
- **THEN** the returned `spark_jobj` Column can be used with sparklyr operations to filter Patient rows

#### Scenario: Expression result can be used in select

- **WHEN** `pc %>% pathling_fhirpath_to_column("Patient", "name.given.first()")` is called
- **THEN** the returned `spark_jobj` Column can be used with sparklyr operations to extract values

#### Scenario: Invalid expression raises an error

- **WHEN** `pc %>% pathling_fhirpath_to_column("Patient", "!!invalid!!")` is called
- **THEN** an R error SHALL be raised

### Requirement: pathling_fhirpath_to_column has roxygen2 documentation

The function SHALL include roxygen2 documentation with a title, description, parameter documentation, return value, and export tag. The `@examples` block SHALL demonstrate filtering using `pathling_filter` instead of raw `j_invoke` calls.

#### Scenario: Documentation is present

- **WHEN** the function is defined
- **THEN** it SHALL have roxygen2 comments documenting its purpose, parameters (`pc`, `resource_type`, `fhirpath_expression`), and return value

#### Scenario: Examples use pathling_filter for filtering

- **WHEN** the roxygen `@examples` block demonstrates filtering a DataFrame
- **THEN** it SHALL use `pathling_filter` and SHALL NOT contain `j_invoke`
