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
