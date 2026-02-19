## ADDED Requirements

### Requirement: FieldGuidance component renders guidance text

The system SHALL provide a `FieldGuidance` component that renders children as small, grey helper text using Radix UI's `Text` component with `size="1"` and `color="gray"`.

#### Scenario: Default rendering

- **WHEN** `FieldGuidance` is rendered with children `"Only resources updated after this time."`
- **THEN** the component SHALL render a `Text` element with `size="1"`, `color="gray"`, and `mt="1"` containing the provided text.

#### Scenario: Custom margin top

- **WHEN** `FieldGuidance` is rendered with `mt="2"`
- **THEN** the component SHALL render with `mt="2"` instead of the default `"1"`.

#### Scenario: No margin top

- **WHEN** `FieldGuidance` is rendered with `mt="0"`
- **THEN** the component SHALL render with no top margin.

### Requirement: All field guidance text uses FieldGuidance component

All inline field guidance text previously rendered as `<Text size="1" color="gray" ...>` SHALL be replaced with the `FieldGuidance` component in the following locations:

- `ExportOptions`: "Since" field, "Until" field, "Elements" field, "Output format" field, and type filters empty state.
- `SearchParamsInput`: search parameters syntax guidance.
- `ResourceSearchForm`: FHIRPath filter guidance.
- `ImportForm`: supported URL schemes guidance.
- `ImportPnpForm`: export URL guidance.
- `SqlOnFhirForm`: view definition selection guidance and custom JSON guidance.
- `ResourceTypePicker`: "leave empty to export all" hint.

#### Scenario: ExportOptions guidance text

- **WHEN** the export options form is rendered
- **THEN** each field guidance string SHALL be rendered using the `FieldGuidance` component with identical text content to the current implementation.

#### Scenario: Visual output unchanged

- **WHEN** any form containing field guidance is rendered after the refactoring
- **THEN** the visual appearance SHALL be identical to the previous inline implementation.
