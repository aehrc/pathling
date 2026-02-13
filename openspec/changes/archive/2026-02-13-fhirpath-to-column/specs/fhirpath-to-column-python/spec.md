## ADDED Requirements

### Requirement: PathlingContext exposes fhirpath_to_column method

The `PathlingContext` class in the Python library SHALL provide a method `fhirpath_to_column(resource_type: str, fhirpath_expression: str) -> Column` that returns a PySpark `Column`. The method SHALL delegate to the Java `PathlingContext.fhirPathToColumn()` method via py4j.

#### Scenario: Boolean expression returns a PySpark Column for filtering

- **WHEN** `pc.fhirpath_to_column("Patient", "gender = 'male'")` is called
- **THEN** the returned PySpark `Column` can be passed to `df.filter()` to filter Patient rows

#### Scenario: Expression result can be used in select

- **WHEN** `pc.fhirpath_to_column("Patient", "name.given.first()")` is called
- **THEN** the returned PySpark `Column` can be passed to `df.select()` to extract values

#### Scenario: Invalid expression raises exception

- **WHEN** `pc.fhirpath_to_column("Patient", "!!invalid!!")` is called
- **THEN** a Python exception SHALL be raised

### Requirement: fhirpath_to_column has comprehensive docstring

The method SHALL include a docstring with a description, parameter documentation, return type documentation, and a usage example.

#### Scenario: Docstring is present

- **WHEN** the method is defined
- **THEN** it SHALL have a docstring describing its purpose, parameters (`resource_type`, `fhirpath_expression`), return value, and at least one usage example
