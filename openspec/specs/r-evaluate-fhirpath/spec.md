## ADDED Requirements

### Requirement: R library exposes pathling_evaluate_fhirpath function

The R library SHALL provide an exported function
`pathling_evaluate_fhirpath(pc, resource_type, resource_json, fhirpath_expression, context_expression, variables)`
that invokes the Java `PathlingContext.evaluateFhirPath()` method via sparklyr's
`j_invoke` and returns an R list containing the typed results and the expected
return type.

The `context_expression` and `variables` parameters SHALL be optional, defaulting
to `NULL`.

#### Scenario: Simple string expression

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "name.family")` is called with a Patient JSON containing family name "Smith"
- **THEN** the returned list contains `results` with one entry having `type = "string"` and `value = "Smith"`, and `expectedReturnType = "string"`

#### Scenario: Expression returning multiple values

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "name.given")` is called with a Patient JSON containing given names "John" and "James"
- **THEN** the returned list contains `results` with two entries, each having `type = "string"`

#### Scenario: Expression returning empty result

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "deceased")` is called with a Patient JSON that has no deceased element
- **THEN** the returned list contains an empty `results` list

#### Scenario: Invalid expression raises an error

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "!!invalid!!")` is called
- **THEN** an R error SHALL be raised

#### Scenario: Context expression evaluation

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "given.first()", context_expression = "name")` is called
- **THEN** the returned list contains results from evaluating the main expression within the context of each name

#### Scenario: Variable substitution

- **WHEN** `pathling_evaluate_fhirpath(pc, "Patient", patient_json, "%myVar", variables = list(myVar = "test"))` is called
- **THEN** the returned list contains `results` with one entry having `type = "string"` and `value = "test"`

### Requirement: Return structure matches Python equivalent

The R function SHALL return a list with the following structure:

- `results`: a list of lists, each containing `type` (character) and `value`
  (the materialised R value or NULL).
- `expectedReturnType`: a character string indicating the inferred return type.

This structure SHALL mirror the Python `evaluate_fhirpath()` return dictionary.

#### Scenario: Structure verification

- **WHEN** any successful evaluation is performed
- **THEN** the result SHALL be an R list with named elements `results` and `expectedReturnType`

#### Scenario: Each result entry has type and value

- **WHEN** the evaluation returns at least one result
- **THEN** each entry in `results` SHALL be a list with named elements `type` (character) and `value`

### Requirement: pathling_evaluate_fhirpath has roxygen2 documentation

The function SHALL include roxygen2 documentation with a title, description,
parameter documentation for all parameters, return value documentation, family
tag, and export tag. The `@examples` block SHALL demonstrate evaluating a simple
expression against a Patient resource.

#### Scenario: Documentation is present

- **WHEN** the function is defined
- **THEN** it SHALL have roxygen2 comments documenting its purpose, all parameters, and return value

### Requirement: Documentation section for single resource evaluation

The `site/docs/libraries/fhirpath.md` page SHALL include a section titled
"Single resource evaluation" that demonstrates evaluating a FHIRPath expression
against a single FHIR resource JSON string. The section SHALL include code
examples in Python, R, Scala, and Java.

#### Scenario: Section is present with all language tabs

- **WHEN** a user reads the FHIRPath documentation page
- **THEN** there SHALL be a "Single resource evaluation" section with tabs for Python, R, Scala, and Java

#### Scenario: Examples show result inspection

- **WHEN** a user reads the code examples
- **THEN** each example SHALL demonstrate calling the evaluate method and accessing the typed results
