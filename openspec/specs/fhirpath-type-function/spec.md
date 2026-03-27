## ADDED Requirements

### Requirement: type() function returns TypeInfo for System primitive literals

The `type()` function SHALL return a `SimpleTypeInfo` with `namespace` set to
`"System"`, `name` set to the FHIRPath type specifier, and `baseType` set to
`"System.Any"` for each element in the input collection when the elements are
System primitive literals.

#### Scenario: Integer literal

- **WHEN** the expression `1.type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "Integer"`, `baseType = "System.Any"`

#### Scenario: Boolean literal

- **WHEN** the expression `true.type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "Boolean"`, `baseType = "System.Any"`

#### Scenario: String literal

- **WHEN** the expression `'hello'.type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "String"`, `baseType = "System.Any"`

#### Scenario: Navigation into TypeInfo fields

- **WHEN** the expression `1.type().namespace` is evaluated
- **THEN** the result is `"System"`
- **WHEN** the expression `1.type().name` is evaluated
- **THEN** the result is `"Integer"`

### Requirement: type() function returns TypeInfo for System Quantity literals

The `type()` function SHALL return a `SimpleTypeInfo` with `namespace` set to
`"System"`, `name` set to `"Quantity"`, and `baseType` set to `"System.Any"`
when the input is a Quantity literal.

#### Scenario: Quantity literal

- **WHEN** the expression `(10 'mg').type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "Quantity"`, `baseType = "System.Any"`

### Requirement: type() function returns TypeInfo for System Coding literals

The `type()` function SHALL return a `SimpleTypeInfo` with `namespace` set to
`"System"`, `name` set to `"Coding"`, and `baseType` set to `"System.Any"`
when the input is a Coding literal.

#### Scenario: Coding literal

- **WHEN** the expression `(http://example.com|code).type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "Coding"`, `baseType = "System.Any"`

### Requirement: type() function returns TypeInfo for FHIR primitive elements

The `type()` function SHALL return a `ClassInfo` with `namespace` set to
`"FHIR"`, `name` set to the FHIR type code, and `baseType` set to
`"FHIR.Element"` when the input elements are FHIR primitive elements navigated
from a resource path.

#### Scenario: FHIR boolean element

- **WHEN** the expression `Patient.active.type()` is evaluated against a
  Patient resource
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "boolean"`, `baseType = "FHIR.Element"`

#### Scenario: FHIR boolean element name navigation

- **WHEN** the expression `Patient.active.type().name` is evaluated
- **THEN** the result is `"boolean"`

#### Scenario: FHIR boolean element namespace navigation

- **WHEN** the expression `Patient.active.type().namespace` is evaluated
- **THEN** the result is `"FHIR"`

### Requirement: type() function returns TypeInfo for FHIR complex type elements

The `type()` function SHALL return a `ClassInfo` with `namespace` set to
`"FHIR"`, `name` set to the FHIR type code, and `baseType` set to
`"FHIR.Element"` when the input elements are FHIR complex type elements.

#### Scenario: FHIR CodeableConcept element

- **WHEN** the expression `Patient.maritalStatus.type()` is evaluated against a
  Patient resource
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "CodeableConcept"`, `baseType = "FHIR.Element"`

### Requirement: type() function returns TypeInfo for FHIR resource types

The `type()` function SHALL return a `ClassInfo` with `namespace` set to
`"FHIR"`, `name` set to the resource type name, and `baseType` set to
`"FHIR.Resource"` when the input is a FHIR resource.

#### Scenario: Patient resource

- **WHEN** the expression `Patient.type()` is evaluated against a Patient
  resource
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "FHIR"`, `name = "Patient"`, `baseType = "FHIR.Resource"`

#### Scenario: Patient resource name navigation

- **WHEN** the expression `Patient.type().name` is evaluated
- **THEN** the result is `"Patient"`

### Requirement: type() function returns TypeInfo for collections with multiple elements

The `type()` function SHALL return one TypeInfo per element in the input
collection.

#### Scenario: Multiple string literals

- **WHEN** the expression `('John' | 'Mary').type()` is evaluated
- **THEN** the result is a collection containing two TypeInfo elements, both
  with `namespace = "System"`, `name = "String"`, `baseType = "System.Any"`

### Requirement: type() returns System type for FHIRPath operation results on FHIR elements

When a FHIRPath operation produces a new value from a FHIR element (e.g.,
`not()`, arithmetic, string functions), the result loses its FHIR context and
becomes a System type. The `type()` function SHALL return the corresponding
System TypeInfo in these cases.

#### Scenario: Boolean negation of FHIR element

- **WHEN** the expression `Patient.active.not().type().namespace` is evaluated
  against a Patient resource
- **THEN** the result is `"System"`
- **WHEN** the expression `Patient.active.not().type().name` is evaluated
- **THEN** the result is `"Boolean"`

#### Scenario: String concatenation of FHIR elements

- **WHEN** the expression `(Patient.name.first().given.first() + Patient.name.first().family).type().name`
  is evaluated against a Patient resource
- **THEN** the result is `"String"` with `namespace = "System"` because the `+`
  operator produces a new System value

#### Scenario: where() preserves FHIR context

- **WHEN** the expression `Patient.active.where($this).type().namespace` is
  evaluated against a Patient resource with `active = true`
- **THEN** the result is `"FHIR"` because `where()` preserves the collection
  type

### Requirement: type() function returns empty for empty collections

The `type()` function SHALL return an empty collection when the input collection
is empty.

#### Scenario: Empty collection

- **WHEN** the expression `{}.type()` is evaluated
- **THEN** the result is an empty collection

### Requirement: type() on TypeInfo returns System.Object

The `type()` function SHALL return a TypeInfo with `namespace` set to
`"System"`, `name` set to `"Object"`, and `baseType` set to `"System.Any"`
when called on the result of a previous `type()` invocation.

#### Scenario: Nested type() call

- **WHEN** the expression `1.type().type()` is evaluated
- **THEN** the result is a collection containing one TypeInfo with
  `namespace = "System"`, `name = "Object"`, `baseType = "System.Any"`

### Requirement: type() works with ofType() filtering

The `type()` function SHALL correctly report type information for collections
that have been filtered using `ofType()`.

#### Scenario: ofType followed by type

- **WHEN** the expression `Patient.ofType(Patient).type().name` is evaluated
- **THEN** the result is `"Patient"`

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
