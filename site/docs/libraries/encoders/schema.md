# Schema specification

This specification describes a scheme for encoding resource definitions from the
FHIR specification as a Spark SQL schema.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "
SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## Version compatibility

The following versions of FHIR are supported by this specification:

- [4.0.1 (R4)](https://hl7.org/fhir/R4)

## Resource type support

All R4 resource types are supported by this specification, except the following:

- Parameters
- Task
- StructureDefinition
- StructureMap
- Bundle

## Configuration options

There are several options that determine the encoding strategy. Schemas that
have been encoded according to this specification with the same configuration
values SHALL be identical and compatible.

| Option                     | Description                                               |
|----------------------------|-----------------------------------------------------------|
| Maximum nesting level      | The maximum depth of nested element data that is encoded. |
| Extension encoding enabled | Whether extension content is included within the schema.  |
| Supported open types       | The list of types that are encoded for open choice types. |

## Out of scope

The following areas are not currently supported by this specification:

- Primitive extensions
- Profiled resources

## Encoding strategy

All columns and struct fields SHALL be encoded as nullable, unless otherwise 
specified.

### Primitive elements

Each primitive element within a resource definition SHALL be encoded as a column
within the schema with the same name (except where otherwise specified). The
data type SHALL be determined by the element type according to the following
table:

| FHIR type       | Spark SQL type | Additional requirements                                                                          |
|-----------------|----------------|--------------------------------------------------------------------------------------------------|
| boolean         | boolean        |                                                                                                  |
| canonical       | string         | Compliant with the [FHIR canonical format](https://hl7.org/fhir/R4/datatypes.html#canonical)     |
| code            | string         | Compliant with the [FHIR code format](https://hl7.org/fhir/R4/datatypes.html#code)               |
| dateTime        | string         | Compliant with the [FHIR dateTime format](https://hl7.org/fhir/R4/datatypes.html#dateTime)       |
| date            | string         | Compliant with the [FHIR date format](https://hl7.org/fhir/R4/datatypes.html#date)               |
| decimal         | decimal(32,6)  | See [Decimal encoding](#decimal-encoding)                                                        |
| id              | string         | See [ID encoding](#id-encoding)                                                                  |
| instant         | timestamp      |                                                                                                  |
| integer         | integer        | Compliant with the [FHIR integer format](https://hl7.org/fhir/R4/datatypes.html#integer)         |
| markdown        | string         | Compliant with the [FHIR markdown format](https://hl7.org/fhir/R4/datatypes.html#markdown)       |
| oid             | string         | Compliant with the [FHIR oid format](https://hl7.org/fhir/R4/datatypes.html#oid)                 |
| positiveInt     | integer        | Compliant with the [FHIR positiveInt format](https://hl7.org/fhir/R4/datatypes.html#positiveInt) |
| string          | string         | Compliant with the [FHIR string format](https://hl7.org/fhir/R4/datatypes.html#string)           |
| time            | string         | Compliant with the [FHIR time format](https://hl7.org/fhir/R4/datatypes.html#time)               |
| unsignedInt     | integer        | Compliant with the [FHIR unsignedInt format](https://hl7.org/fhir/R4/datatypes.html#unsignedInt) |
| uri             | string         | Compliant with the [FHIR uri format](https://hl7.org/fhir/R4/datatypes.html#uri)                 |
| url             | string         | Compliant with the [FHIR url format](https://hl7.org/fhir/R4/datatypes.html#url)                 |
| uuid            | string         | Compliant with the [FHIR uuid format](https://hl7.org/fhir/R4/datatypes.html#uuid)               |

#### Decimal encoding

An element of type `decimal` SHALL be encoded as a decimal column with a 
precision of 32 and a scale of 6.

In addition to this column, an integer column SHALL be encoded with the
suffix `_scale`. This column SHALL contain the scale of the decimal value from 
the original FHIR data.

#### ID encoding

An element of type `id` SHALL be encoded as a string column. This column SHALL 
contain the FHIR resource logical ID.

In addition to this column, a string column SHALL be encoded with the
suffix `_versioned`. This column SHALL contain a fully qualified logical ID,
including the resource type and the technical version. The data in this column
SHALL follow the format `[resource type]/[logical ID]/_history/[version]`.

### Choice types

If the choice type is open (i.e. a type of `*`), the struct SHALL contain a 
field for each type listed in the configuration value "supported open types".

If the choice type is not open, it SHALL be encoded as a struct column with a
field for each of its valid types.

The name of each field SHALL follow the format `value[type]`, where `[type]` is
the name of the type in upper camel case.

### Complex and backbone elements

Each complex and backbone element within a resource definition SHALL be encoded
as a struct column within the schema with the same name. The struct SHALL
contain fields for each of the child elements of the complex or backbone
element. 

The encoding rules for each field SHALL follow the same rules as described in
the [primitive elements](#primitive-elements) and [choice types](#choice-types) 
section (and this section in the case of nested complex or backbone elements).

If the "extension encoding enabled" option is set to true, an additional column
SHALL be encoded with the name `_fid`. This column SHALL have a type of integer.
This column SHALL contain a value that uniquely identifies the complex or 
backbone element within the resource.

#### Quantity encoding

If the complex element is of
type [Quantity](https://hl7.org/fhir/R4/datatypes.html#Quantity), an additional
two columns SHALL be encoded as part of the struct. These columns SHALL be
named `_value_canonicalized` and `_code_canonicalized`.

The `_value_canonicalized` column SHALL be encoded as a struct with the 
following fields:

- `value` with a type of decimal, precision 38 and scale 0.
- `scale` with a type of integer.

The `_code_canonicalized` column SHALL be encoded as a string column.

Implementations loading data into this schema MAY use these columns to store
canonicalized versions of the `value` and `code` fields from the original FHIR
data, for easier comparison and querying.

### Extension encoding

If the "extension encoding enabled" option is set to true, an additional column
SHALL be encoded at the root of the schema named `_extension`. This column SHALL
have a type of map, with an integer key and an array value. 

The array SHALL contain a struct type that is encoded as
the [Extension](https://hl7.org/fhir/R4/extensibility.html#extension) type, as
described in [Complex and backbone elements](#complex-and-backbone-elements).

Implementations loading data into this schema SHALL use this column to store
extension data from the original FHIR data (except for primitive extensions).
The map SHALL contain a reference to the `_fid` of the element that the
extension is attached to. 

## Example

The following schema is an example of the encoding of
the [Patient](https://hl7.org/fhir/R4/patient.html) resource type with the
following configuration values:

- Maximum nesting level: `3`
- Extension encoding enabled: `true`
- Supported open types: `boolean`, `code`, `date`, `dateTime`, `decimal`,
  `integer`, `string`, `Coding`, `CodeableConcept`, `Address`, `Identifier`,
  `Reference`

```
root
 |-- id: string (nullable = true)
 |-- id_versioned: string (nullable = true)
 |-- meta: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- versionId: string (nullable = true)
 |    |-- versionId_versioned: string (nullable = true)
 |    |-- lastUpdated: timestamp (nullable = true)
 |    |-- source: string (nullable = true)
 |    |-- profile: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- security: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- system: string (nullable = true)
 |    |    |    |-- version: string (nullable = true)
 |    |    |    |-- code: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |-- tag: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- system: string (nullable = true)
 |    |    |    |-- version: string (nullable = true)
 |    |    |    |-- code: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |-- _fid: integer (nullable = true)
 |-- implicitRules: string (nullable = true)
 |-- language: string (nullable = true)
 |-- text: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- status: string (nullable = true)
 |    |-- div: string (nullable = true)
 |    |-- _fid: integer (nullable = true)
 |-- identifier: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- type: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |    |    |-- period: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- start: string (nullable = true)
 |    |    |    |-- end: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- assigner: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- active: boolean (nullable = true)
 |-- name: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- family: string (nullable = true)
 |    |    |-- given: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- prefix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- suffix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- period: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- start: string (nullable = true)
 |    |    |    |-- end: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- telecom: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- rank: integer (nullable = true)
 |    |    |-- period: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- start: string (nullable = true)
 |    |    |    |-- end: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- birthDate: string (nullable = true)
 |-- deceasedBoolean: boolean (nullable = true)
 |-- deceasedDateTime: string (nullable = true)
 |-- address: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- line: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- district: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- postalCode: string (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- period: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- start: string (nullable = true)
 |    |    |    |-- end: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- maritalStatus: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- coding: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- system: string (nullable = true)
 |    |    |    |-- version: string (nullable = true)
 |    |    |    |-- code: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |-- text: string (nullable = true)
 |    |-- _fid: integer (nullable = true)
 |-- multipleBirthBoolean: boolean (nullable = true)
 |-- multipleBirthInteger: integer (nullable = true)
 |-- photo: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- contentType: string (nullable = true)
 |    |    |-- language: string (nullable = true)
 |    |    |-- data: binary (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- size: integer (nullable = true)
 |    |    |-- hash: binary (nullable = true)
 |    |    |-- title: string (nullable = true)
 |    |    |-- creation: string (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- contact: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- relationship: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- name: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- use: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- family: string (nullable = true)
 |    |    |    |-- given: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- prefix: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- suffix: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- period: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- start: string (nullable = true)
 |    |    |    |    |-- end: string (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- telecom: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |-- value: string (nullable = true)
 |    |    |    |    |-- use: string (nullable = true)
 |    |    |    |    |-- rank: integer (nullable = true)
 |    |    |    |    |-- period: struct (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- start: string (nullable = true)
 |    |    |    |    |    |-- end: string (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- address: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- use: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- line: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- city: string (nullable = true)
 |    |    |    |-- district: string (nullable = true)
 |    |    |    |-- state: string (nullable = true)
 |    |    |    |-- postalCode: string (nullable = true)
 |    |    |    |-- country: string (nullable = true)
 |    |    |    |-- period: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- start: string (nullable = true)
 |    |    |    |    |-- end: string (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- gender: string (nullable = true)
 |    |    |-- organization: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- period: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- start: string (nullable = true)
 |    |    |    |-- end: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- communication: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- language: struct (nullable = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- preferred: boolean (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- generalPractitioner: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- reference: string (nullable = true)
 |    |    |-- display: string (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- managingOrganization: struct (nullable = true)
 |    |-- reference: string (nullable = true)
 |    |-- display: string (nullable = true)
 |    |-- _fid: integer (nullable = true)
 |-- link: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- other: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- _fid: integer (nullable = true)
 |-- _fid: integer (nullable = true)
 |-- _extension: map (nullable = true)
 |    |-- key: integer
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- url: string (nullable = true)
 |    |    |    |-- valueAddress: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- use: string (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |    |-- line: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- city: string (nullable = true)
 |    |    |    |    |-- district: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- postalCode: string (nullable = true)
 |    |    |    |    |-- country: string (nullable = true)
 |    |    |    |    |-- period: struct (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- start: string (nullable = true)
 |    |    |    |    |    |-- end: string (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- valueBoolean: boolean (nullable = true)
 |    |    |    |-- valueCode: string (nullable = true)
 |    |    |    |-- valueCodeableConcept: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- valueCoding: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- valueDateTime: string (nullable = true)
 |    |    |    |-- valueDate: string (nullable = true)
 |    |    |    |-- valueDecimal: decimal(32,6) (nullable = true)
 |    |    |    |-- valueDecimal_scale: integer (nullable = true)
 |    |    |    |-- valueIdentifier: struct (nullable = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- use: string (nullable = true)
 |    |    |    |    |-- type: struct (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |    |    |-- version: string (nullable = true)
 |    |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |    |    |-- userSelected: boolean (nullable = true)
 |    |    |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |-- value: string (nullable = true)
 |    |    |    |    |-- period: struct (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |    |-- start: string (nullable = true)
 |    |    |    |    |    |-- end: string (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- assigner: struct (nullable = true)
 |    |    |    |    |    |-- reference: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- valueInteger: integer (nullable = true)
 |    |    |    |-- valueReference: struct (nullable = true)
 |    |    |    |    |-- reference: string (nullable = true)
 |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |-- _fid: integer (nullable = true)
 |    |    |    |-- valueString: string (nullable = true)
 |    |    |    |-- _fid: integer (nullable = true)
```
