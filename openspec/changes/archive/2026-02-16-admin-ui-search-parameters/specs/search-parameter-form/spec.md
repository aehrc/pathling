## ADDED Requirements

### Requirement: Search parameters extracted from CapabilityStatement

The `parseCapabilities` function SHALL extract search parameter definitions from
each resource's `searchParam` array in the CapabilityStatement. Each search
parameter SHALL include its name and type. The `ResourceCapability` interface
SHALL be extended to include a `searchParams` field containing an array of
`{ name: string; type: string }` objects.

#### Scenario: CapabilityStatement contains search parameters for Patient

- **WHEN** the CapabilityStatement declares search parameters `gender` (token),
  `birthdate` (date), and `name` (string) for the Patient resource
- **THEN** the parsed `ResourceCapability` for Patient SHALL include a
  `searchParams` array containing entries for `gender` with type `token`,
  `birthdate` with type `date`, and `name` with type `string`

#### Scenario: CapabilityStatement has resource with no search parameters

- **WHEN** the CapabilityStatement declares a resource type with no
  `searchParam` entries
- **THEN** the parsed `ResourceCapability` for that resource SHALL have an
  empty `searchParams` array

### Requirement: Search parameter section in search form

The resource search form SHALL include a "Search parameters" section positioned
between the resource type selector and the FHIRPath filters section. This
section SHALL allow users to add rows, where each row consists of a parameter
name dropdown and a value text input.

#### Scenario: Initial form state

- **WHEN** the search form is first rendered
- **THEN** the search parameters section SHALL be displayed with a heading, an
  "Add parameter" button, and one empty parameter row

#### Scenario: User adds a search parameter row

- **WHEN** the user clicks the "Add parameter" button
- **THEN** a new parameter row SHALL be appended with an empty parameter
  dropdown and an empty value input

#### Scenario: User removes a search parameter row

- **WHEN** the user clicks the remove button on a parameter row and there is
  more than one row
- **THEN** that row SHALL be removed from the form

#### Scenario: Last parameter row cannot be removed

- **WHEN** there is only one parameter row
- **THEN** the remove button on that row SHALL be disabled

### Requirement: Parameter dropdown populated by resource type

The parameter name dropdown in each search parameter row SHALL display the
search parameters available for the currently selected resource type, as
extracted from the CapabilityStatement. Each option SHALL show the parameter
name, with the parameter type displayed as a badge next to it.

#### Scenario: Parameter list updates on resource type change

- **WHEN** the user changes the selected resource type from Patient to
  Observation
- **THEN** the parameter dropdown options SHALL update to show search parameters
  declared for Observation, and any previously selected parameters that are not
  valid for Observation SHALL be cleared

#### Scenario: No search parameters available

- **WHEN** the selected resource type has no declared search parameters
- **THEN** the parameter dropdown SHALL be empty and display placeholder text
  indicating no parameters are available

### Requirement: Search parameters included in search request

When the user submits the search form, any search parameter rows with both a
selected parameter name and a non-empty value SHALL be included in the search
request as standard FHIR query parameters alongside any FHIRPath filters.

#### Scenario: Search with standard parameters only

- **WHEN** the user selects resource type "Patient", adds parameter "gender"
  with value "male", leaves the FHIRPath filter empty, and submits the search
- **THEN** the search request SHALL be sent as
  `GET [base]/Patient?gender=male&_count=10`

#### Scenario: Search combining standard parameters and FHIRPath filters

- **WHEN** the user selects resource type "Patient", adds parameter "gender"
  with value "male", adds FHIRPath filter `active = true`, and submits the
  search
- **THEN** the search request SHALL be sent as
  `GET [base]/Patient?_query=fhirPath&filter=active = true&gender=male&_count=10`

#### Scenario: Empty parameter rows are excluded

- **WHEN** the user has a parameter row with no parameter selected or no value
  entered, and submits the search
- **THEN** that row SHALL be excluded from the search request

#### Scenario: Multiple values for the same parameter

- **WHEN** the user adds two rows for the same parameter name with different
  values
- **THEN** both values SHALL be sent as repeated query parameters (AND logic),
  e.g., `date=gt2020-01-01&date=lt2025-01-01`

### Requirement: SearchRequest type extended for standard parameters

The `SearchRequest` interface SHALL be extended to include a `params` field of
type `Record<string, string[]>` to carry standard search parameter name-value
pairs alongside the existing `filters` array.

#### Scenario: SearchRequest with both filters and params

- **WHEN** a search is submitted with FHIRPath filter `active = true` and
  search parameter `gender=male`
- **THEN** the `SearchRequest` object SHALL contain
  `filters: ["active = true"]` and `params: { gender: ["male"] }`

### Requirement: Help text for search parameters section

The search parameters section SHALL include help text explaining that search
parameters use standard FHIR search syntax and are combined with AND logic.

#### Scenario: Help text is displayed

- **WHEN** the search form is rendered
- **THEN** the search parameters section SHALL display help text below the
  parameter rows explaining the AND combination logic and that values use
  standard FHIR search syntax
