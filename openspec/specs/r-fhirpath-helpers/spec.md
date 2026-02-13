## Requirements

### Requirement: pathling_filter filters a DataFrame using a FHIRPath expression

The R library SHALL provide an exported function `pathling_filter(df, pc, resource_type, expression, type = "fhirpath")` that filters a `tbl_spark` using a FHIRPath boolean expression and returns a `tbl_spark`.

#### Scenario: Filter patients by gender using FHIRPath

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "gender = 'male'")` is called
- **THEN** the returned `tbl_spark` SHALL contain only Patient rows where gender equals male

#### Scenario: Filter with combined boolean expression

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "gender = 'male' and birthDate > @1970-01-01")` is called
- **THEN** the returned `tbl_spark` SHALL contain only Patient rows matching both conditions

#### Scenario: Invalid FHIRPath expression raises error

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "!!invalid!!")` is called
- **THEN** an R error SHALL be raised

### Requirement: pathling_filter filters a DataFrame using a FHIR search expression

The `pathling_filter` function SHALL accept `type = "search"` to filter using a FHIR search query string instead of a FHIRPath expression.

#### Scenario: Filter patients by gender using search syntax

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "gender=male", type = "search")` is called
- **THEN** the returned `tbl_spark` SHALL contain only Patient rows where gender equals male

#### Scenario: Filter with multiple search parameters

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "gender=male&birthdate=ge1990-01-01", type = "search")` is called
- **THEN** the returned `tbl_spark` SHALL contain only Patient rows matching both conditions

### Requirement: pathling_with_column adds a FHIRPath-derived column to a DataFrame

The R library SHALL provide an exported function `pathling_with_column(df, pc, resource_type, expression, column)` that evaluates a FHIRPath expression and adds the result as a named column, returning a `tbl_spark`.

#### Scenario: Add a value column

- **WHEN** `patients %>% pathling_with_column(pc, "Patient", "name.given.first()", column = "given_name")` is called
- **THEN** the returned `tbl_spark` SHALL have a column named `given_name` containing the first given name of each patient

#### Scenario: Chain multiple columns

- **WHEN** multiple `pathling_with_column` calls are piped together
- **THEN** each call SHALL add its column to the DataFrame, and the result SHALL be usable with `dplyr::select`

#### Scenario: Invalid expression raises error

- **WHEN** `patients %>% pathling_with_column(pc, "Patient", "!!invalid!!", column = "bad")` is called
- **THEN** an R error SHALL be raised

### Requirement: Both functions follow sparklyr conventions

Both `pathling_filter` and `pathling_with_column` SHALL accept the DataFrame as their first argument to enable piping with `%>%`, and SHALL return a `tbl_spark` object.

#### Scenario: Functions are pipeable

- **WHEN** `patients %>% pathling_filter(pc, "Patient", "gender = 'male'") %>% pathling_with_column(pc, "Patient", "name.given.first()", column = "given") %>% select(id, given)` is called
- **THEN** the result SHALL be a `tbl_spark` with `id` and `given` columns for male patients only

### Requirement: Both functions have roxygen2 documentation

Both functions SHALL include roxygen2 documentation with title, description, parameter documentation, return value, family tag, export tag, and usage examples.

#### Scenario: Documentation is present

- **WHEN** the functions are defined
- **THEN** they SHALL have roxygen2 comments documenting their purpose, all parameters, return value, and at least one usage example
