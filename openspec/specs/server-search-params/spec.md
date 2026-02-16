## ADDED Requirements

### Requirement: Standard search parameters on resource type endpoints

The server SHALL accept standard FHIR search parameters directly on resource
type search endpoints without requiring a named query. The server SHALL support
all search parameter types that `SearchColumnBuilder` supports: token, string,
date, number, quantity, reference, and URI.

For string-type search parameters that resolve to complex types, the server
SHALL decompose the type into its string sub-fields and match against each one:

- **HumanName**: `family`, `given`, `text`, `prefix`, `suffix`
- **Address**: `text`, `line`, `city`, `district`, `state`, `postalCode`,
  `country`

A match on any sub-field SHALL satisfy the search criterion.

#### Scenario: Search by token parameter

- **WHEN** a client sends `GET /Patient?gender=male`
- **THEN** the server returns a Bundle containing only Patient resources where
  `gender` equals `male`

#### Scenario: Search by date parameter with prefix

- **WHEN** a client sends `GET /Patient?birthdate=ge1990-01-01`
- **THEN** the server returns a Bundle containing only Patient resources where
  `birthDate` is on or after 1990-01-01

#### Scenario: Search by string parameter

- **WHEN** a client sends `GET /Patient?name=eve`
- **THEN** the server returns a Bundle containing only Patient resources where
  any name component starts with "eve" (case-insensitive)

#### Scenario: Search by string parameter matching family name

- **WHEN** a client sends `GET /Patient?name=smith`
- **THEN** the server returns a Bundle containing only Patient resources where
  the family name, given name, prefix, suffix, or text of any HumanName starts
  with "smith" (case-insensitive)

#### Scenario: Search by string parameter matching given name

- **WHEN** a client sends `GET /Patient?name=john`
- **THEN** the server returns a Bundle containing only Patient resources where
  the family name, given name, prefix, suffix, or text of any HumanName starts
  with "john" (case-insensitive)

#### Scenario: Search by address parameter

- **WHEN** a client sends `GET /Patient?address=spring`
- **THEN** the server returns a Bundle containing only Patient resources where
  any address component (text, line, city, district, state, postalCode, or
  country) starts with "spring" (case-insensitive)

#### Scenario: Search by exact string parameter on complex type

- **WHEN** a client sends `GET /Patient?name:exact=Smith`
- **THEN** the server returns a Bundle containing only Patient resources where
  any name component exactly matches "Smith" (case-sensitive)

#### Scenario: Search by reference parameter

- **WHEN** a client sends `GET /Observation?subject=Patient/123`
- **THEN** the server returns a Bundle containing only Observation resources
  where the subject references Patient/123

#### Scenario: Search by number parameter

- **WHEN** a client sends `GET /RiskAssessment?probability=gt0.8`
- **THEN** the server returns a Bundle containing only RiskAssessment resources
  where the probability is greater than 0.8

#### Scenario: Search by quantity parameter

- **WHEN** a client sends `GET /Observation?value-quantity=5.4|http://unitsofmeasure.org|mg`
- **THEN** the server returns a Bundle containing only Observation resources
  where the value quantity matches the specified value, system, and code

#### Scenario: Search by URI parameter

- **WHEN** a client sends `GET /ValueSet?url=http://example.org/fhir/ValueSet/my-valueset`
- **THEN** the server returns a Bundle containing only ValueSet resources where
  the URL matches exactly

#### Scenario: Search via POST

- **WHEN** a client sends `POST /Patient/_search` with form body
  `gender=male&birthdate=ge1990-01-01`
- **THEN** the server returns a Bundle containing matching Patient resources,
  equivalent to the GET form

### Requirement: FHIR combining semantics for standard parameters

The server SHALL follow FHIR search combining rules: repeated parameters are
combined with AND logic, and comma-separated values within a single parameter
are combined with OR logic.

#### Scenario: AND logic between repeated parameters

- **WHEN** a client sends `GET /Patient?gender=male&birthdate=ge1990-01-01`
- **THEN** the server returns only Patient resources matching both criteria

#### Scenario: OR logic within comma-separated values

- **WHEN** a client sends `GET /Patient?gender=male,female`
- **THEN** the server returns Patient resources where gender is either `male`
  or `female`

### Requirement: Combine standard parameters with FHIRPath filters

The server SHALL allow standard search parameters and FHIRPath filter
expressions in the same request. All criteria SHALL be combined using AND logic.
FHIRPath filters require the `_query=fhirPath` named query parameter.

#### Scenario: Standard parameter combined with FHIRPath filter

- **WHEN** a client sends
  `GET /Patient?gender=male&_query=fhirPath&filter=birthDate%20%3E%20%401980-01-01`
- **THEN** the server returns only Patient resources where gender is male AND
  birthDate is after 1980-01-01

#### Scenario: Multiple standard parameters with multiple FHIRPath filters

- **WHEN** a client sends a request with standard parameters `gender=male` and
  `active=true`, plus FHIRPath filters `birthDate > @1980-01-01` and
  `name.given.count() > 0`
- **THEN** the server returns only Patient resources matching all four criteria

### Requirement: Backwards-compatible FHIRPath-only search

The server SHALL continue to support the existing `_query=fhirPath` named query
with `filter` parameters, with identical behaviour to the current
implementation.

#### Scenario: FHIRPath search without standard parameters

- **WHEN** a client sends
  `GET /Patient?_query=fhirPath&filter=gender%3D'male'`
- **THEN** the server returns a Bundle matching the FHIRPath expression,
  identical to the current behaviour

#### Scenario: Multiple FHIRPath filters with AND and OR

- **WHEN** a client sends a request with multiple `filter` parameters
  (AND logic) and comma-separated expressions within a single parameter
  (OR logic)
- **THEN** the server applies the same AND/OR combining behaviour as
  the current implementation

### Requirement: Search parameter modifier support

The server SHALL support search parameter modifiers as implemented by
`SearchColumnBuilder`, including `:not`, `:exact`, `:below`, `:above`, and
resource type modifiers for references.

#### Scenario: Token parameter with not modifier

- **WHEN** a client sends `GET /Patient?gender:not=male`
- **THEN** the server returns Patient resources where gender is not `male`

#### Scenario: String parameter with exact modifier

- **WHEN** a client sends `GET /Patient?name:exact=Eve`
- **THEN** the server returns Patient resources where a name component exactly
  matches "Eve" (case-sensitive)

#### Scenario: URI parameter with below modifier

- **WHEN** a client sends `GET /ValueSet?url:below=http://example.org/fhir/`
- **THEN** the server returns ValueSet resources where the URL starts with
  `http://example.org/fhir/`

### Requirement: Error handling for unsupported parameters

The server SHALL return an appropriate error response when a client uses a
search parameter that is not in the registry for the target resource type.

#### Scenario: Unknown search parameter

- **WHEN** a client sends `GET /Patient?unknownparam=value`
- **THEN** the server returns a `400 Bad Request` response with an
  OperationOutcome describing the unsupported parameter

#### Scenario: Unsupported modifier

- **WHEN** a client sends `GET /Patient?gender:contains=male`
- **THEN** the server returns a `400 Bad Request` response with an
  OperationOutcome describing the unsupported modifier

#### Scenario: Standard parameters on non-registry resource type

- **WHEN** a client sends `GET /ViewDefinition?name=foo`
- **THEN** the server returns a `400 Bad Request` response with an
  OperationOutcome indicating that standard search parameters are not supported
  for the ViewDefinition resource type

### Requirement: CapabilityStatement declares supported search parameters

The `ConformanceProvider` SHALL declare all search parameters from the search
parameter registry in the CapabilityStatement, with the correct parameter type,
for each resource type that Pathling supports.

#### Scenario: CapabilityStatement includes search parameters for Patient

- **WHEN** a client requests `GET /metadata`
- **THEN** the returned CapabilityStatement includes search parameter
  declarations for the Patient resource type, including at minimum `gender`
  (token), `birthdate` (date), `name` (string), and `identifier` (token), each
  with the correct FHIR search parameter type

#### Scenario: CapabilityStatement includes FHIRPath filter documentation

- **WHEN** a client requests `GET /metadata`
- **THEN** the returned CapabilityStatement continues to include the `filter`
  parameter documentation for the `_query=fhirPath` named query alongside the
  standard search parameters

#### Scenario: All resource types have search parameters declared

- **WHEN** a client requests `GET /metadata`
- **THEN** every resource type in the CapabilityStatement includes its
  applicable search parameters from the registry

### Requirement: Search results include pagination

The server SHALL continue to support pagination in search results, including
when standard search parameters are used. The `_count` parameter SHALL work
with standard search parameters.

#### Scenario: Paginated results with standard parameters

- **WHEN** a client sends `GET /Patient?gender=male&_count=10`
- **THEN** the server returns at most 10 Patient resources per page with
  appropriate pagination links

### Requirement: Unfiltered search without named query

The server SHALL return all resources of the requested type when no search
parameters and no `_query` parameter are provided.

#### Scenario: Unfiltered search

- **WHEN** a client sends `GET /Patient`
- **THEN** the server returns a Bundle containing all Patient resources
  (subject to pagination)
