## MODIFIED Requirements

### Requirement: Search parameter section in search form

The resource search form SHALL include a "Search parameters" section positioned
between the resource type selector and the FHIRPath filters section. This
section SHALL render a `SearchParamsInput` component that allows users to add
rows, where each row consists of a parameter name dropdown and a value text
input. The search form SHALL manage the row state and pass the available
parameters and rows to the `SearchParamsInput` component.

#### Scenario: Initial form state

- **WHEN** the search form is first rendered
- **THEN** the search parameters section SHALL be displayed using the
  `SearchParamsInput` component with one empty parameter row

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

## ADDED Requirements

### Requirement: Reusable SearchParamsInput component

The search parameter rows UI (parameter dropdown, value input, add/remove
controls, and help text) SHALL be provided as a reusable `SearchParamsInput`
component at `ui/src/components/SearchParamsInput.tsx`. The component SHALL
accept the following props:

- `availableParams`: array of `{ name: string; type: string }` objects to
  populate the parameter dropdown.
- `rows`: array of `{ id: number; paramName: string; value: string }` objects
  representing the current row state.
- `onChange`: callback invoked with the updated rows array when the user adds,
  removes, or edits a row.
- `onKeyDown` (optional): keyboard event handler to attach to value inputs.

The component SHALL be stateless, receiving all state from the parent and
reporting changes via the `onChange` callback.

#### Scenario: SearchParamsInput renders rows from props

- **WHEN** `SearchParamsInput` is rendered with `rows` containing two entries
  and `availableParams` containing three parameters
- **THEN** two parameter rows SHALL be displayed, each with a dropdown
  containing three options and a value text input

#### Scenario: SearchParamsInput reports row addition

- **WHEN** the user clicks the "Add parameter" button within `SearchParamsInput`
- **THEN** the `onChange` callback SHALL be invoked with the current rows plus a
  new empty row appended

#### Scenario: SearchParamsInput reports row removal

- **WHEN** the user clicks the remove button on a row and there is more than one
  row
- **THEN** the `onChange` callback SHALL be invoked with the clicked row removed

#### Scenario: SearchParamsInput disables remove on last row

- **WHEN** `SearchParamsInput` is rendered with a single row
- **THEN** the remove button SHALL be disabled

#### Scenario: SearchParamsInput reports parameter name change

- **WHEN** the user selects a parameter name from the dropdown
- **THEN** the `onChange` callback SHALL be invoked with the updated row
  reflecting the new parameter name

#### Scenario: SearchParamsInput reports value change

- **WHEN** the user types a value in a row's text input
- **THEN** the `onChange` callback SHALL be invoked with the updated row
  reflecting the new value
