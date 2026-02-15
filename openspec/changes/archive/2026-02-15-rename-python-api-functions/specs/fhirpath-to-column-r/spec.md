## RENAMED Requirements

### Requirement: R library exposes pc_fhirpath_to_column function

**FROM**: `pc_fhirpath_to_column`
**TO**: `pathling_fhirpath_to_column`

### Requirement: pc_fhirpath_to_column has roxygen2 documentation

**FROM**: `pc_fhirpath_to_column`
**TO**: `pathling_fhirpath_to_column`

## MODIFIED Requirements

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

The function SHALL include roxygen2 documentation with a title, description, parameter documentation, return value, and export tag.

#### Scenario: Documentation is present

- **WHEN** the function is defined
- **THEN** it SHALL have roxygen2 comments documenting its purpose, parameters (`pc`, `resource_type`, `fhirpath_expression`), and return value
