## MODIFIED Requirements

### Requirement: Evaluate FHIRPath expression against a single resource

The library API SHALL provide a method that accepts a FHIR resource (as a JSON
string), a FHIRPath expression string, an optional context expression string,
and optional variables, and returns a list of typed result values.

The method SHALL use the existing Pathling FHIRPath engine (Spark-based) to
evaluate the expression, encoding the resource into a one-row Dataset
internally.

When the expression is empty or contains only whitespace, the fhirpath-lab-api
SHALL return a successful response with an empty collection (zero result parts)
without invoking the FHIRPath engine.

#### Scenario: Simple expression evaluation

- **WHEN** the method is called with a Patient JSON resource and the expression
  `name.family`
- **THEN** the method returns a list containing a single string result with the
  patient's family name

#### Scenario: Expression returning multiple values

- **WHEN** the method is called with a Patient JSON resource containing two
  names and the expression `name.given`
- **THEN** the method returns a list containing all given name strings

#### Scenario: Expression returning empty result

- **WHEN** the method is called with a Patient JSON resource and an expression
  that matches no elements (e.g., `deceased`)
- **THEN** the method returns an empty list

#### Scenario: Empty expression

- **WHEN** the fhirpath-lab-api receives a request with an expression that is an
  empty string or contains only whitespace
- **THEN** it returns a successful Parameters response with zero result parts and
  no type metadata, without invoking the FHIRPath engine

#### Scenario: Invalid expression

- **WHEN** the method is called with a syntactically invalid FHIRPath expression
- **THEN** the method throws an exception with a descriptive error message

#### Scenario: Context expression evaluation

- **WHEN** the method is called with a Patient JSON resource, context expression
  `name`, and expression `given.first()`
- **THEN** the method evaluates the main expression once for each result of the
  context expression and returns results grouped by context item
