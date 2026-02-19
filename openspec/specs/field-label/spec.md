## ADDED Requirements

### Requirement: FieldLabel component renders a form field label

The system SHALL provide a `FieldLabel` component that renders children as a
label using Radix UI's `Text` component with `as="label"`, `size="2"`, and
`weight="medium"`.

#### Scenario: Default rendering

- **WHEN** `FieldLabel` is rendered with children `"Resource type"`
- **THEN** the component SHALL render a `Text` element with `as="label"`,
  `size="2"`, `weight="medium"`, and `mb="1"` containing the text
  "Resource type".

#### Scenario: Custom bottom margin

- **WHEN** `FieldLabel` is rendered with `mb="2"`
- **THEN** the component SHALL render with `mb="2"` instead of the default
  `"1"`.

#### Scenario: No bottom margin

- **WHEN** `FieldLabel` is rendered with `mb="0"`
- **THEN** the component SHALL render with no bottom margin.

### Requirement: FieldLabel supports an optional marker

When the `optional` prop is `true`, the component SHALL append a grey
" (optional)" suffix after the label text.

#### Scenario: Optional marker displayed

- **WHEN** `FieldLabel` is rendered with children `"Elements"` and
  `optional={true}`
- **THEN** the rendered output SHALL display "Elements" followed by " (optional)"
  in a smaller, grey style (`size="1"`, `color="gray"`).

#### Scenario: Optional marker not displayed by default

- **WHEN** `FieldLabel` is rendered with children `"Resource type"` without
  the `optional` prop
- **THEN** no optional marker SHALL be rendered.

### Requirement: All field labels use the FieldLabel component

All inline field labels previously rendered as
`<Text as="label" size="2" weight="medium" ...>` SHALL be replaced with the
`FieldLabel` component in the following locations:

- `ExportForm`: "Export level", "Patient ID", "Group ID".
- `ExportOptions`: "Since", "Until", "Elements (optional)", "Output format",
  "Type filters".
- `ResourceTypePicker`: "Resource types".
- `ImportForm`: "Input format", "Input files".
- `ImportPnpForm`: "Input format", "Export URL".
- `SaveModeField`: "Save mode".
- `SqlOnFhirForm`: "View definition", "View definition JSON".
- `ResourceSearchForm`: "Resource type", "FHIRPath filters".
- `SearchParamsInput`: "Search parameters".
- `BulkSubmitMonitorForm`: "Submitter system", "Submitter value",
  "Submission ID".

#### Scenario: Visual output unchanged

- **WHEN** any form containing a field label is rendered after the refactoring
- **THEN** the visual appearance SHALL be identical to the previous inline
  implementation.
