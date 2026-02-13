## ADDED Requirements

### Requirement: Match URI by exact string equality

The URI search parameter type SHALL match a search value against the element value using exact, case-sensitive string equality.

#### Scenario: Exact URL match

- **WHEN** an element has value `http://hl7.org/fhir/StructureDefinition/Patient` and the search value is `http://hl7.org/fhir/StructureDefinition/Patient`
- **THEN** the matcher SHALL return true.

#### Scenario: URN UUID exact match

- **WHEN** an element has value `urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7` and the search value is `urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7`
- **THEN** the matcher SHALL return true.

#### Scenario: Case mismatch does not match

- **WHEN** an element has value `http://example.org/fhir` and the search value is `http://example.org/FHIR`
- **THEN** the matcher SHALL return false.

#### Scenario: Partial URI does not match

- **WHEN** an element has value `http://example.org/fhir/StructureDefinition/Patient` and the search value is `http://example.org/fhir`
- **THEN** the matcher SHALL return false.

#### Scenario: Different URI does not match

- **WHEN** an element has value `http://example.org/fhir` and the search value is `http://other.org/fhir`
- **THEN** the matcher SHALL return false.

### Requirement: URI type is registered with all URI family FHIR types

The `URI` search parameter type SHALL declare `FHIRDefinedType.URI`, `FHIRDefinedType.URL`, `FHIRDefinedType.CANONICAL`, `FHIRDefinedType.OID`, and `FHIRDefinedType.UUID` as its allowed FHIR types. This is necessary because the FHIRPath evaluator preserves the specific subtype from HAPI's element definitions rather than resolving to the `uri` base type.

#### Scenario: URI search parameter resolving to FHIRDefinedType.URI

- **WHEN** a search query uses a URI-type parameter whose expression resolves to `FHIRDefinedType.URI` (e.g., `instantiates-uri` on CarePlan)
- **THEN** the search column builder SHALL create a filter column without error.

#### Scenario: URI search parameter resolving to FHIRDefinedType.URL

- **WHEN** a search query uses a URI-type parameter whose expression resolves to `FHIRDefinedType.URL` (e.g., `url` on CapabilityStatement)
- **THEN** the search column builder SHALL create a filter column without error.

### Requirement: `:below` modifier matches by URI prefix

The URI search parameter type SHALL support the `:below` modifier, which matches when the element value starts with the search value.

#### Scenario: Prefix match with `:below`

- **WHEN** an element has value `http://example.org/fhir/ValueSet/my-valueset` and the search is `url:below=http://example.org/fhir/`
- **THEN** the matcher SHALL return true.

#### Scenario: Non-matching prefix with `:below`

- **WHEN** an element has value `http://other.org/fhir/ValueSet/my-valueset` and the search is `url:below=http://example.org/fhir/`
- **THEN** the matcher SHALL return false.

#### Scenario: Exact value matches with `:below`

- **WHEN** an element has value `http://example.org/fhir` and the search is `url:below=http://example.org/fhir`
- **THEN** the matcher SHALL return true, since the element value starts with the search value.

### Requirement: `:above` modifier matches by inverse prefix

The URI search parameter type SHALL support the `:above` modifier, which matches when the element value is a prefix of the search value. This finds resources whose URI is "above" the search value in the URI hierarchy.

#### Scenario: Element is prefix of search value with `:above`

- **WHEN** an element has value `http://example.org/fhir` and the search is `url:above=http://example.org/fhir/ValueSet/my-valueset`
- **THEN** the matcher SHALL return true.

#### Scenario: Element is not a prefix of search value with `:above`

- **WHEN** an element has value `http://other.org/fhir` and the search is `url:above=http://example.org/fhir/ValueSet/my-valueset`
- **THEN** the matcher SHALL return false.

#### Scenario: Exact value matches with `:above`

- **WHEN** an element has value `http://example.org/fhir` and the search is `url:above=http://example.org/fhir`
- **THEN** the matcher SHALL return true, since the search value starts with the element value.

### Requirement: `:not` modifier negates matching

The URI search parameter type SHALL support the `:not` modifier, which negates the match result. Resources whose URI matches any of the provided values SHALL be excluded. This is a Pathling extension not defined for URI in FHIR R4, but consistent with other Pathling search parameter types.

#### Scenario: `:not` modifier excludes matching URIs

- **WHEN** a search is performed with `url:not=http://example.org/fhir/StructureDefinition/Patient`
- **THEN** resources with that URL SHALL be excluded, and resources with other URLs SHALL be included.

### Requirement: Unsupported modifiers are rejected

The URI search parameter type SHALL throw `InvalidModifierException` for any modifier other than `not`, `below`, and `above`.

#### Scenario: `:exact` modifier is rejected

- **WHEN** a URI search parameter is used with the `:exact` modifier
- **THEN** the system SHALL throw `InvalidModifierException`.

#### Scenario: `:contains` modifier is rejected

- **WHEN** a URI search parameter is used with the `:contains` modifier
- **THEN** the system SHALL throw `InvalidModifierException`.

### Requirement: Multiple values use OR logic

When multiple comma-separated values are provided for a URI search parameter, the filter SHALL match if ANY value matches (OR semantics), consistent with all other search parameter types.

#### Scenario: OR across multiple URI values

- **WHEN** a search is performed with `url=http://example.org/a,http://example.org/b`
- **THEN** resources matching either URI SHALL be included.

### Requirement: Null element does not match

When the element value is null, the matcher SHALL not match any search value.

#### Scenario: Null element value

- **WHEN** an element has a null value and the search value is `http://example.org`
- **THEN** the matcher SHALL return false.
