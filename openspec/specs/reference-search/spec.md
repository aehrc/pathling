## ADDED Requirements

### Requirement: Match reference by type-qualified ID

The reference search parameter type SHALL match a search value of the form `[type]/[id]` against the `reference` field of a FHIR Reference element. A match occurs when the reference string ends with the search value, preceded by either the start of the string or a `/` character.

#### Scenario: Relative reference exactly equals type-qualified search

- **WHEN** a Reference element has `reference` value `Patient/123` and the search value is `Patient/123`
- **THEN** the matcher SHALL return true.

#### Scenario: Absolute reference matches type-qualified search

- **WHEN** a Reference element has `reference` value `http://example.org/fhir/Patient/123` and the search value is `Patient/123`
- **THEN** the matcher SHALL return true.

#### Scenario: Type mismatch does not match

- **WHEN** a Reference element has `reference` value `Practitioner/123` and the search value is `Patient/123`
- **THEN** the matcher SHALL return false.

#### Scenario: ID mismatch does not match

- **WHEN** a Reference element has `reference` value `Patient/456` and the search value is `Patient/123`
- **THEN** the matcher SHALL return false.

#### Scenario: Partial type name does not match

- **WHEN** a Reference element has `reference` value `http://example.org/fhir/APatient/123` and the search value is `Patient/123`
- **THEN** the matcher SHALL return false.

### Requirement: Match reference by bare ID

The reference search parameter type SHALL match a search value consisting of a bare ID (no `/` separator) by checking that the reference string ends with `/<id>`.

#### Scenario: Bare ID matches relative reference

- **WHEN** a Reference element has `reference` value `Patient/123` and the search value is `123`
- **THEN** the matcher SHALL return true.

#### Scenario: Bare ID matches absolute reference

- **WHEN** a Reference element has `reference` value `http://example.org/fhir/Patient/123` and the search value is `123`
- **THEN** the matcher SHALL return true.

#### Scenario: Bare ID does not match partial ID

- **WHEN** a Reference element has `reference` value `Patient/1234` and the search value is `123`
- **THEN** the matcher SHALL return false.

### Requirement: Match reference by absolute URI

The reference search parameter type SHALL match a search value that is an absolute URI (starting with `http://`, `https://`, `urn:uuid:`, or `urn:oid:`) by performing exact string equality against the `reference` field.

#### Scenario: Exact absolute URL match

- **WHEN** a Reference element has `reference` value `http://example.org/fhir/Patient/123` and the search value is `http://example.org/fhir/Patient/123`
- **THEN** the matcher SHALL return true.

#### Scenario: Different server URL does not match

- **WHEN** a Reference element has `reference` value `http://other.org/fhir/Patient/123` and the search value is `http://example.org/fhir/Patient/123`
- **THEN** the matcher SHALL return false.

#### Scenario: URN UUID exact match

- **WHEN** a Reference element has `reference` value `urn:uuid:a4f9d12b-3e7c-4f8a-9b2d-1c6e8f0a3d5b` and the search value is `urn:uuid:a4f9d12b-3e7c-4f8a-9b2d-1c6e8f0a3d5b`
- **THEN** the matcher SHALL return true.

### Requirement: Reference type is registered with allowed FHIR types

The `REFERENCE` search parameter type SHALL declare `FHIRDefinedType.REFERENCE` as its allowed FHIR type, enabling the search framework to validate and dispatch reference search parameters.

#### Scenario: Reference search parameters are loaded from registry

- **WHEN** a search query uses a reference-type parameter (e.g., `subject=Patient/123` on Observation)
- **THEN** the search column builder SHALL create a filter column without throwing an unsupported operation error.

### Requirement: `:not` modifier negates matching

The reference search parameter type SHALL support the `:not` modifier, which negates the match result. Resources whose reference matches any of the provided values SHALL be excluded.

#### Scenario: `:not` modifier excludes matching references

- **WHEN** a search is performed with `subject:not=Patient/123`
- **THEN** resources referencing `Patient/123` SHALL be excluded, and resources referencing other patients SHALL be included.

### Requirement: `:[type]` modifier constrains reference type

The reference search parameter type SHALL support the `:[type]` modifier, where `[type]` is a valid FHIR resource type name. When present, the modifier prepends `[type]/` to bare ID search values before matching. For example, `subject:Patient=123` is equivalent to `subject=Patient/123`.

#### Scenario: Type modifier with bare ID

- **WHEN** a search is performed with `subject:Patient=123`
- **THEN** resources referencing `Patient/123` SHALL be included, but resources referencing `Group/123` SHALL be excluded.

#### Scenario: Type modifier with already type-qualified value

- **WHEN** a search is performed with `subject:Patient=Patient/123`
- **THEN** the value is already type-qualified, so matching proceeds as normal against `Patient/123`.

### Requirement: Unsupported modifiers are rejected

The reference search parameter type SHALL throw `InvalidModifierException` for any modifier that is not `not` and is not a valid FHIR resource type name.

#### Scenario: Unsupported modifier on reference parameter

- **WHEN** a reference search parameter is used with an unsupported modifier (e.g., `subject:exact=Patient/123`)
- **THEN** the system SHALL throw `InvalidModifierException`.

### Requirement: Multiple values use OR logic

When multiple comma-separated values are provided for a reference search parameter, the filter SHALL match if ANY value matches (OR semantics), consistent with all other search parameter types.

#### Scenario: OR across multiple reference values

- **WHEN** a search is performed with `subject=Patient/123,Patient/456`
- **THEN** resources referencing either `Patient/123` or `Patient/456` SHALL be included.

### Requirement: Null reference does not match

When the `reference` field is null, the matcher SHALL not match any search value.

#### Scenario: Null reference field

- **WHEN** a Reference element has a null `reference` field and the search value is `Patient/123`
- **THEN** the matcher SHALL return false.
