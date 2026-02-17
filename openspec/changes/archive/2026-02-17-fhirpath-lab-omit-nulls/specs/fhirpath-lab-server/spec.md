## MODIFIED Requirements

### Requirement: Result formatting

Each evaluation result SHALL be returned as a `result` part in the output
Parameters resource. The part name of each value within a result SHALL be the
FHIR data type name (e.g., `string`, `integer`, `boolean`, `HumanName`).

Complex type results SHALL be sanitised before serialisation to remove synthetic
fields that are not part of the FHIR specification. A field is synthetic if its
name starts with `_` or ends with `_scale`, consistent with the
`PruneSyntheticFields.isAnnotation` convention in the encoders module.

Complex type results SHALL also have null-valued fields stripped before
serialisation, so that the JSON representation only contains fields with
non-null values. This stripping SHALL be applied recursively to nested structs.

This sanitisation SHALL be applied recursively to nested struct types within
complex type results.

#### Scenario: Primitive result formatting

- **WHEN** the expression returns primitive values (e.g., strings)
- **THEN** each value is a part with name equal to the type and the appropriate
  `value[x]` property (e.g., `valueString`)

#### Scenario: Complex type result formatting

- **WHEN** the expression returns complex types (e.g., HumanName) that cannot
  be represented as FHIR Parameters value types
- **THEN** each value uses the
  `http://fhir.forms-lab.com/StructureDefinition/json-value` extension with
  the value serialised as JSON

#### Scenario: Complex type results exclude synthetic fields

- **WHEN** the expression returns a Quantity value
- **THEN** the JSON representation does not contain `value_scale`,
  `_value_canonicalized`, or `_code_canonicalized` fields

#### Scenario: Complex type results exclude \_fid field

- **WHEN** the expression returns any complex type
- **THEN** the JSON representation does not contain a `_fid` field

#### Scenario: Nested struct sanitisation

- **WHEN** the expression returns a complex type containing nested struct
  fields with synthetic fields
- **THEN** the synthetic fields are stripped from nested structs as well

#### Scenario: Complex type results exclude null-valued fields

- **WHEN** the expression returns a Quantity value with `id`, `comparator`, and
  other fields set to null
- **THEN** the JSON representation omits those null-valued fields entirely

#### Scenario: Nested null stripping

- **WHEN** the expression returns a complex type containing nested structs with
  null-valued fields
- **THEN** the null-valued fields are stripped from nested structs as well

#### Scenario: Empty result

- **WHEN** the expression evaluates to an empty collection
- **THEN** the response contains no `result` parts (only the `parameters` part)

#### Scenario: Context-scoped results

- **WHEN** a context expression is provided that yields multiple items
- **THEN** the response contains one `result` part per context item, each with
  a `valueString` indicating the context path
