## ADDED Requirements

### Requirement: type() function returns per-row TypeInfo for singular choice elements

The `type()` function SHALL return a `ClassInfo` with `namespace` set to
`"FHIR"`, `name` set to the concrete FHIR type code of the non-null choice
field, and `baseType` set to `"FHIR.Element"` when the input is a singular
choice element (e.g., accessed via indexing). The concrete type is determined at
runtime based on which choice field is non-null for each row.

#### Scenario: Singular choice element with Quantity value

- **WHEN** the expression `component[0].value.type()` is evaluated against an
  Observation where the first component's value is a Quantity
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "Quantity"`, `baseType = "FHIR.Element"`

#### Scenario: Singular choice element with string value

- **WHEN** the expression `component[2].value.type()` is evaluated against an
  Observation where the third component's value is a string
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "string"`, `baseType = "FHIR.Element"`

#### Scenario: Singular choice element with boolean value

- **WHEN** the expression `component[4].value.type()` is evaluated against an
  Observation where the fifth component's value is a boolean
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "boolean"`, `baseType = "FHIR.Element"`

#### Scenario: Singular choice element with CodeableConcept value

- **WHEN** the expression `component[3].value.type()` is evaluated against an
  Observation where the fourth component's value is a CodeableConcept
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "CodeableConcept"`, `baseType = "FHIR.Element"`

#### Scenario: Navigation into TypeInfo fields of singular choice element

- **WHEN** the expression `component[0].value.type().name` is evaluated against
  an Observation where the first component's value is a Quantity
- **THEN** the result is `"Quantity"`
- **WHEN** the expression `component[0].value.type().namespace` is evaluated
- **THEN** the result is `"FHIR"`
- **WHEN** the expression `component[0].value.type().baseType` is evaluated
- **THEN** the result is `"FHIR.Element"`

### Requirement: type() function returns per-row TypeInfo for plural choice collections

The `type()` function SHALL return one TypeInfo per element in a plural choice
collection, with each TypeInfo reflecting the concrete type of the
corresponding element. Different elements in the same collection MAY have
different types.

#### Scenario: Plural choice collection with heterogeneous types

- **WHEN** the expression `component.value.type()` is evaluated against an
  Observation with five components having value types Quantity, Quantity, string,
  CodeableConcept, and boolean respectively
- **THEN** the result is a collection containing five TypeInfo elements:
    - `{namespace = "FHIR", name = "Quantity", baseType = "FHIR.Element"}`
    - `{namespace = "FHIR", name = "Quantity", baseType = "FHIR.Element"}`
    - `{namespace = "FHIR", name = "string", baseType = "FHIR.Element"}`
    - `{namespace = "FHIR", name = "CodeableConcept", baseType = "FHIR.Element"}`
    - `{namespace = "FHIR", name = "boolean", baseType = "FHIR.Element"}`

#### Scenario: Plural choice collection count

- **WHEN** the expression `component.value.type().count()` is evaluated against
  an Observation with five components each having a value
- **THEN** the result is `5`

#### Scenario: Plural choice collection name navigation

- **WHEN** the expression `component.value.type().name` is evaluated against an
  Observation with components having Quantity, Quantity, string,
  CodeableConcept, and boolean values
- **THEN** the result is a collection containing `"Quantity"`, `"Quantity"`,
  `"string"`, `"CodeableConcept"`, `"boolean"`

### Requirement: type() on choice element with no value returns null element

The `type()` function SHALL return a null element within the collection for a
choice element where all choice fields are null (no value is set).

#### Scenario: Choice element with no value set

- **WHEN** the expression `component[0].value.type()` is evaluated against an
  Observation where the first component has no value set
- **THEN** the result is a collection containing one null element

### Requirement: type() on choice element composes with other FHIRPath functions

The TypeInfo returned by `type()` on choice elements SHALL be usable with
standard FHIRPath functions such as `where()`, `select()`, and equality
comparisons.

#### Scenario: Filtering by type name using where()

- **WHEN** the expression
  `component.where(value.type().name = 'Quantity').count()` is evaluated against
  an Observation with two Quantity components and three non-Quantity components
- **THEN** the result is `2`

#### Scenario: Distinct type names

- **WHEN** the expression `component.value.type().name.distinct().count()` is
  evaluated against an Observation with components having Quantity, Quantity,
  string, CodeableConcept, and boolean values
- **THEN** the result is `4`
