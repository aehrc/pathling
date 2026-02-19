## ADDED Requirements

### Requirement: Accept \_typeFilter parameter on export endpoints

The server SHALL accept the `_typeFilter` query parameter on all three export endpoints (`/$export`, `/Patient/$export`, `/Group/[id]/$export`). Each `_typeFilter` value SHALL be a string in the format `[ResourceType]?[search-params]`, where `[ResourceType]` is a valid FHIR resource type code and `[search-params]` is a valid FHIR search query string.

#### Scenario: Valid \_typeFilter on system export

- **WHEN** a client sends `GET /$export?_type=Observation&_typeFilter=Observation?code=8867-4` with `Prefer: respond-async` and `Accept: application/fhir+json`
- **THEN** the server SHALL return 202 Accepted with a `Content-Location` header

#### Scenario: Valid \_typeFilter on patient export

- **WHEN** a client sends `GET /Patient/$export?_type=MedicationRequest&_typeFilter=MedicationRequest?status=active` with `Prefer: respond-async` and `Accept: application/fhir+json`
- **THEN** the server SHALL return 202 Accepted with a `Content-Location` header

#### Scenario: Valid \_typeFilter on group export

- **WHEN** a client sends `GET /Group/123/$export?_type=Condition&_typeFilter=Condition?code=73211009` with `Prefer: respond-async` and `Accept: application/fhir+json`
- **THEN** the server SHALL return 202 Accepted with a `Content-Location` header

### Requirement: Parse \_typeFilter resource type and search query

The server SHALL parse each `_typeFilter` value by splitting on the first `?` character. The portion before the `?` SHALL be interpreted as the resource type code. The portion after the `?` SHALL be interpreted as a FHIR search query string.

#### Scenario: Parse resource type and search query

- **WHEN** a `_typeFilter` value is `Observation?code=8867-4&date=ge2024-01-01`
- **THEN** the server SHALL parse resource type as `Observation` and search query as `code=8867-4&date=ge2024-01-01`

#### Scenario: Reject \_typeFilter without question mark separator

- **WHEN** a `_typeFilter` value does not contain a `?` character (e.g., `Observation`)
- **THEN** the server SHALL return 400 Bad Request with an OperationOutcome describing the invalid format

#### Scenario: Reject \_typeFilter with empty search query

- **WHEN** a `_typeFilter` value has an empty search query after the `?` (e.g., `Observation?`)
- **THEN** the server SHALL return 400 Bad Request with an OperationOutcome describing the invalid format

### Requirement: Validate \_typeFilter resource types

The server SHALL validate that the resource type in each `_typeFilter` value is a valid, supported FHIR resource type. Invalid resource types SHALL be handled according to the strict/lenient mode.

#### Scenario: Invalid resource type in strict mode

- **WHEN** a `_typeFilter` value references an invalid resource type (e.g., `FakeResource?status=active`) without lenient handling
- **THEN** the server SHALL return 400 Bad Request with an OperationOutcome

#### Scenario: Invalid resource type in lenient mode

- **WHEN** a `_typeFilter` value references an invalid resource type with `Prefer: handling=lenient`
- **THEN** the server SHALL ignore the `_typeFilter` value and include an informational OperationOutcome issue in the response

### Requirement: Validate \_typeFilter search parameters

The server SHALL validate that the search parameters within each `_typeFilter` value are recognised search parameters for the specified resource type. Unknown search parameters SHALL be handled according to the strict/lenient mode.

#### Scenario: Unknown search parameter in strict mode

- **WHEN** a `_typeFilter` value contains an unknown search parameter (e.g., `Patient?unknownParam=value`) without lenient handling
- **THEN** the server SHALL return 400 Bad Request with an OperationOutcome

#### Scenario: Unknown search parameter in lenient mode

- **WHEN** a `_typeFilter` value contains an unknown search parameter with `Prefer: handling=lenient`
- **THEN** the server SHALL ignore the individual `_typeFilter` value and include an informational OperationOutcome issue

### Requirement: Apply \_typeFilter as row-level search filter

The server SHALL apply each `_typeFilter` value as a row-level filter during export execution. Only resources matching the search criteria SHALL be included in the export output for that resource type.

#### Scenario: Filter observations by code

- **WHEN** an export is requested with `_typeFilter=Observation?code=8867-4`
- **THEN** the exported Observation resources SHALL include only those with a code matching `8867-4`, and the output SHALL NOT contain Observation resources with other codes

#### Scenario: Filter with multiple search criteria (AND logic)

- **WHEN** an export is requested with `_typeFilter=Observation?code=8867-4&date=ge2024-01-01`
- **THEN** the exported Observation resources SHALL include only those matching both criteria (code `8867-4` AND date on or after 2024-01-01)

#### Scenario: No matching resources

- **WHEN** an export is requested with a `_typeFilter` that matches no resources
- **THEN** the export SHALL complete successfully, and the output manifest SHALL not include a file entry for that resource type

### Requirement: Combine multiple \_typeFilter values for same resource type with OR logic

When multiple `_typeFilter` values target the same resource type, the server SHALL combine them with OR logic. A resource is included if it matches any one of the filters for its type.

#### Scenario: OR combination of filters for same type

- **WHEN** an export is requested with `_typeFilter=Observation?code=8867-4&_typeFilter=Observation?code=8310-5`
- **THEN** the exported Observation resources SHALL include those matching code `8867-4` OR code `8310-5`

### Requirement: Implicit resource type inclusion from \_typeFilter

When `_type` is not provided but `_typeFilter` is, the server SHALL include the resource types referenced in the `_typeFilter` values as the effective resource type filter.

#### Scenario: \_typeFilter without \_type implies resource types

- **WHEN** an export is requested with `_typeFilter=Observation?code=8867-4&_typeFilter=Condition?code=73211009` and no `_type` parameter
- **THEN** the export SHALL include Observation and Condition resources (filtered by their respective criteria) and SHALL NOT include other resource types

### Requirement: \_typeFilter consistency with \_type parameter

When both `_type` and `_typeFilter` are provided, `_typeFilter` values that reference resource types not listed in `_type` SHALL be handled according to the strict/lenient mode.

#### Scenario: \_typeFilter type not in \_type in strict mode

- **WHEN** an export is requested with `_type=Patient&_typeFilter=Observation?code=8867-4` without lenient handling
- **THEN** the server SHALL return 400 Bad Request because `Observation` is not included in `_type`

#### Scenario: \_typeFilter type not in \_type in lenient mode

- **WHEN** an export is requested with `_type=Patient&_typeFilter=Observation?code=8867-4` with `Prefer: handling=lenient`
- **THEN** the server SHALL ignore the `Observation` type filter, export only Patient resources, and include an informational OperationOutcome issue

#### Scenario: \_typeFilter matches subset of \_type

- **WHEN** an export is requested with `_type=Patient,Observation&_typeFilter=Observation?code=8867-4`
- **THEN** the export SHALL include all Patient resources (unfiltered) and only Observation resources matching code `8867-4`

### Requirement: \_typeFilter values with resource types not in Patient compartment are handled during patient and group exports

For patient-level and group-level exports, `_typeFilter` values that reference resource types not in the Patient compartment SHALL be handled consistently with existing `_type` behaviour (silently ignored).

#### Scenario: Non-compartment resource type in patient export \_typeFilter

- **WHEN** a patient export is requested with `_typeFilter=Organization?name=test`
- **THEN** the server SHALL silently ignore the `Organization` type filter since Organization is not in the Patient compartment
