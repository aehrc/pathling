## ADDED Requirements

### Requirement: PathlingContext exposes fhirPathToColumn method

The `PathlingContext` class SHALL provide a public method `fhirPathToColumn(String resourceType, String fhirPathExpression)` that returns an `org.apache.spark.sql.Column`. The method SHALL evaluate the given FHIRPath expression against the specified resource type using the existing FHIRPath engine and return a Spark column suitable for use in DataFrame operations.

#### Scenario: Boolean FHIRPath expression returns a filter column

- **WHEN** `fhirPathToColumn("Patient", "gender = 'male'")` is called
- **THEN** the returned Column evaluates to `true` for rows where the Patient's gender is `male` and `false` otherwise

#### Scenario: String FHIRPath expression returns a value column

- **WHEN** `fhirPathToColumn("Patient", "name.given.first()")` is called
- **THEN** the returned Column contains the first given name for each Patient row

#### Scenario: Column can be used with DataFrame filter

- **WHEN** the returned Column is passed to `dataset.filter(column)`
- **THEN** the DataFrame is filtered to only rows where the expression evaluates to `true`

#### Scenario: Column can be used with DataFrame select

- **WHEN** the returned Column is passed to `dataset.select(column)`
- **THEN** the DataFrame contains the evaluated expression values for each row

#### Scenario: Invalid FHIRPath expression throws exception

- **WHEN** `fhirPathToColumn("Patient", "!!invalid!!")` is called
- **THEN** the method SHALL throw an appropriate exception indicating the expression is invalid

#### Scenario: Invalid resource type throws exception

- **WHEN** `fhirPathToColumn("InvalidResource", "gender")` is called
- **THEN** the method SHALL throw an appropriate exception indicating the resource type is invalid

### Requirement: fhirPathToColumn is annotated with nullability

The `fhirPathToColumn` method SHALL use `@Nonnull` annotations on its return type and both parameters, consistent with existing `PathlingContext` methods.

#### Scenario: Method signature includes nullability annotations

- **WHEN** the method is declared
- **THEN** the return type, `resourceType` parameter, and `fhirPathExpression` parameter SHALL all be annotated with `@Nonnull`
