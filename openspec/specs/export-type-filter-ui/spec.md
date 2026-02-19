## ADDED Requirements

### Requirement: Type filters section in export form

The export form SHALL include a "Type filters" section positioned between the
export options and the submit button. The section SHALL display a heading, help
text explaining that type filters restrict exported resources using FHIR search
parameters, and an "Add type filter" button.

#### Scenario: Initial export form has no type filter entries

- **WHEN** the export form is first rendered
- **THEN** the type filters section SHALL be displayed with a heading, help text,
  and an "Add type filter" button, but no type filter entries

#### Scenario: User adds a type filter entry

- **WHEN** the user clicks the "Add type filter" button
- **THEN** a new type filter entry SHALL be appended containing a resource type
  dropdown (defaulting to no selection), a search parameters input with one empty
  row, and a remove button

### Requirement: Type filter entry resource type selection

Each type filter entry SHALL include a resource type dropdown populated with the
available resource types from the server capabilities. When the user selects a
resource type, the search parameters input for that entry SHALL update to show
the search parameters available for the selected type.

#### Scenario: Resource type selection populates search parameters

- **WHEN** the user selects "Observation" in a type filter entry's resource type
  dropdown
- **THEN** the search parameters dropdown for that entry SHALL display the search
  parameters declared for Observation in the CapabilityStatement

#### Scenario: Changing resource type resets search parameter rows

- **WHEN** the user has entered search parameter values for a type filter entry
  and then changes the resource type
- **THEN** all search parameter rows for that entry SHALL be reset to a single
  empty row

### Requirement: Type filter entry search parameters

Each type filter entry SHALL include a search parameters input that allows the
user to add parameter name-value rows, identical in behaviour to the search
parameters section on the Resources page. Each row SHALL have a parameter name
dropdown and a value text input. An "Add parameter" button SHALL allow adding
rows, and each row SHALL have a remove button (disabled when only one row
remains).

#### Scenario: Add search parameter row within a type filter entry

- **WHEN** the user clicks the "Add parameter" button within a type filter entry
- **THEN** a new parameter row SHALL be appended to that entry's search
  parameters input

#### Scenario: Remove search parameter row within a type filter entry

- **WHEN** the user clicks the remove button on a parameter row within a type
  filter entry and there is more than one row
- **THEN** that row SHALL be removed from the entry

### Requirement: Remove type filter entry

The user SHALL be able to remove any type filter entry by clicking its remove
button.

#### Scenario: Remove a type filter entry

- **WHEN** the user clicks the remove button on a type filter entry
- **THEN** that entry SHALL be removed from the type filters section

### Requirement: Type filters included in export request

When the user submits the export form, type filter entries with a selected
resource type and at least one non-empty search parameter row SHALL be serialised
into `_typeFilter` query parameters in the format
`ResourceType?param1=value1&param2=value2` and included in the export API
request. Entries with no resource type selected or with all empty parameter rows
SHALL be excluded.

#### Scenario: Export with a single type filter

- **WHEN** the user adds a type filter entry with resource type "Patient" and
  parameter "active" with value "true", and submits the export
- **THEN** the export API request SHALL include the query parameter
  `_typeFilter=Patient?active=true`

#### Scenario: Export with multiple type filters for different types

- **WHEN** the user adds a type filter entry for "Patient" with parameter
  "active" = "true" and another entry for "Observation" with parameter "code" =
  "8867-4", and submits the export
- **THEN** the export API request SHALL include
  `_typeFilter=Patient?active=true` and `_typeFilter=Observation?code=8867-4`

#### Scenario: Export with multiple type filters for the same type (OR logic)

- **WHEN** the user adds two type filter entries both for "Observation", one with
  parameter "code" = "8867-4" and another with parameter "code" = "8310-5", and
  submits the export
- **THEN** the export API request SHALL include
  `_typeFilter=Observation?code=8867-4` and
  `_typeFilter=Observation?code=8310-5`

#### Scenario: Type filter entry with multiple search parameters (AND logic)

- **WHEN** the user adds a type filter entry for "Observation" with parameters
  "code" = "8867-4" and "date" = "ge2024-01-01", and submits the export
- **THEN** the export API request SHALL include
  `_typeFilter=Observation?code=8867-4&date=ge2024-01-01`

#### Scenario: Incomplete type filter entries are excluded

- **WHEN** the user adds a type filter entry with no resource type selected and
  submits the export
- **THEN** that entry SHALL be excluded from the export API request

#### Scenario: Type filter entry with empty parameter rows is excluded

- **WHEN** the user adds a type filter entry with resource type "Patient" but
  all parameter rows have empty names or values, and submits the export
- **THEN** that entry SHALL be excluded from the export API request

### Requirement: ExportRequest type extended for type filters

The `ExportRequest` interface SHALL include a `typeFilters` field of type
`Array<{ resourceType: string; params: Record<string, string[]> }>` to carry
structured type filter data from the form to the export card.

#### Scenario: ExportRequest with type filters

- **WHEN** an export is submitted with a type filter for "Patient" with parameter
  "active" = "true"
- **THEN** the `ExportRequest` object SHALL contain
  `typeFilters: [{ resourceType: "Patient", params: { active: ["true"] } }]`

### Requirement: BulkExportRequest and API support for typeFilters

The `BulkExportRequest` interface SHALL include a `typeFilters` field of type
`string[]` containing pre-serialised `_typeFilter` strings. The
`BulkExportBaseOptions` interface SHALL include a `typeFilters` field of type
`string[]`. The `buildExportParams` function SHALL include `_typeFilter` entries
in the query parameters sent to the server, supporting multiple values.

#### Scenario: API request includes typeFilter query parameters

- **WHEN** a bulk export is kicked off with
  `typeFilters: ["Patient?active=true", "Observation?code=8867-4"]`
- **THEN** the HTTP request to the server SHALL include query parameters
  `_typeFilter=Patient?active=true&_typeFilter=Observation?code=8867-4`

### Requirement: Remove raw typeFilters and showExtendedOptions from ExportOptions

The `ExportOptions` component SHALL NOT render the raw type filters text field
or the include associated data text field. The `showExtendedOptions` prop SHALL
be removed. The `typeFilters` and `includeAssociatedData` fields SHALL be removed
from the `ExportOptionsValues` interface and `DEFAULT_EXPORT_OPTIONS`.

#### Scenario: ExportOptions does not render type filters text field

- **WHEN** the `ExportOptions` component is rendered
- **THEN** there SHALL be no text field for type filters or include associated
  data
