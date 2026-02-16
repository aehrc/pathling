## MODIFIED Requirements

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
