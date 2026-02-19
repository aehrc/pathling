## MODIFIED Requirements

### Requirement: Type filters section in export form

The `ExportOptions` component SHALL include a "Type filters" section positioned
after the output format field. The section SHALL display a heading, help text
explaining that type filters restrict exported resources using FHIR search
parameters, and an "Add type filter" button. The `ExportOptions` component SHALL
accept an optional `searchParams` prop providing per-resource-type search
parameter metadata.

#### Scenario: Initial export options has no type filter entries

- **WHEN** the `ExportOptions` component is rendered with default values
- **THEN** the type filters section SHALL be displayed with a heading, help
  text, and an "Add type filter" button, but no type filter entries

#### Scenario: User adds a type filter entry

- **WHEN** the user clicks the "Add type filter" button within `ExportOptions`
- **THEN** a new type filter entry SHALL be appended containing a resource type
  dropdown (defaulting to no selection), a search parameters input with one
  empty row, and a remove button
- **AND** the `onChange` callback SHALL be invoked with the updated
  `ExportOptionsValues` including the new entry in `typeFilters`

### Requirement: Type filter entry resource type selection

Each type filter entry SHALL include a resource type dropdown populated with the
available resource types from the server capabilities. When the user selects a
resource type, the search parameters input for that entry SHALL update to show
the search parameters available for the selected type.

#### Scenario: Resource type selection populates search parameters

- **WHEN** the user selects "Observation" in a type filter entry's resource type
  dropdown
- **THEN** the search parameters dropdown for that entry SHALL display the
  search parameters declared for Observation in the CapabilityStatement

#### Scenario: Changing resource type resets search parameter rows

- **WHEN** the user has entered search parameter values for a type filter entry
  and then changes the resource type
- **THEN** all search parameter rows for that entry SHALL be reset to a single
  empty row

### Requirement: Type filter entry search parameters

Each type filter entry SHALL include a search parameters input that allows the
user to add parameter name-value rows. Each row SHALL have a parameter name
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
- **AND** the `onChange` callback SHALL be invoked with the updated
  `ExportOptionsValues` excluding the removed entry

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

#### Scenario: Incomplete type filter entries are excluded

- **WHEN** the user adds a type filter entry with no resource type selected and
  submits the export
- **THEN** that entry SHALL be excluded from the export API request

### Requirement: ExportOptionsValues extended with typeFilters

The `ExportOptionsValues` interface SHALL include a `typeFilters` field of type
`TypeFilterState[]`. The `TypeFilterState` type SHALL be defined in
`exportOptions.ts` with fields `id` (string), `resourceType` (string), and
`rows` (SearchParamRowData[]). The `DEFAULT_EXPORT_OPTIONS` constant SHALL
include `typeFilters: []`.

#### Scenario: Default export options include empty typeFilters

- **WHEN** `DEFAULT_EXPORT_OPTIONS` is used as the initial value
- **THEN** the `typeFilters` field SHALL be an empty array

### Requirement: ImportPnpForm passes type filters to import request

The `ImportPnpForm` SHALL pass `searchParams` through to `ExportOptions` so that
type filter search parameter dropdowns are populated. When the form is submitted,
type filter entries from `exportOptions.typeFilters` SHALL be serialised into
`string[]` format and included in the `ImportPnpRequest.typeFilters` field.

#### Scenario: Import PnP form submits with type filters

- **WHEN** the user configures a type filter for "Patient" with parameter
  "active" = "true" in the import PnP form and submits
- **THEN** the `ImportPnpRequest` SHALL include
  `typeFilters: ["Patient?active=true"]`

#### Scenario: Import PnP form without type filters

- **WHEN** the user submits the import PnP form with no type filters configured
- **THEN** the `ImportPnpRequest` SHALL have `typeFilters` as `undefined`

### Requirement: Remove raw typeFilters and showExtendedOptions from ExportOptions

The `ExportOptions` component SHALL NOT render the raw type filters text field
or the include associated data text field. The `showExtendedOptions` prop SHALL
be removed. The `typeFilters` and `includeAssociatedData` fields SHALL be
removed from the `ExportOptionsValues` interface and `DEFAULT_EXPORT_OPTIONS`.

#### Scenario: ExportOptions does not render type filters text field

- **WHEN** the `ExportOptions` component is rendered
- **THEN** there SHALL be no text field for type filters or include associated
  data
