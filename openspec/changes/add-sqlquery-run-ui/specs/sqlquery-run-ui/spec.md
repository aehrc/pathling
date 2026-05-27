## ADDED Requirements

### Requirement: SQL on FHIR mode selector

The SQL on FHIR page SHALL render a top-level mode selector that switches
between two operations: "View definition" (which invokes
`$viewdefinition-run`, the existing behaviour) and "SQL query" (which
invokes `$sqlquery-run`). The selector SHALL be visible at the top of the
page above the form card and SHALL persist for the lifetime of the page.
The selector SHALL be implemented as a Radix Themes `Tabs.Root`
component, mirroring the top-level mode-switch pattern used on the
Import page.

#### Scenario: Page renders with View definition mode active by default

- **WHEN** the SQL on FHIR page is rendered
- **THEN** the mode selector SHALL be visible with two options "View
  definition" and "SQL query"
- **AND** the "View definition" option SHALL be selected by default
- **AND** the existing ViewDefinition form SHALL be displayed in the form
  card

#### Scenario: User switches to SQL query mode

- **WHEN** the user selects "SQL query" in the mode selector
- **THEN** the form card SHALL replace its body with the SQL query form
- **AND** any result cards already in the result column SHALL remain
  visible
- **AND** the page SHALL not navigate to a different route

#### Scenario: User switches back to View definition mode

- **WHEN** the user selects "View definition" after having been in "SQL
  query" mode
- **THEN** the form card SHALL display the ViewDefinition form
- **AND** SQL query result cards already in the result column SHALL remain
  visible alongside any ViewDefinition result cards

### Requirement: SQL query Library source tabs

The SQL query form SHALL provide two tabs that select the source of the
SQLQuery Library: "Select query" (a stored Library is referenced by
ID) and "Provide SQL" (the Library is authored inline). The tabs
SHALL be implemented as a Radix Themes `Tabs.Root` component nested
inside the form card.

#### Scenario: Tabs are visible in SQL query mode

- **WHEN** the user is in SQL query mode
- **THEN** the form card SHALL display two tabs: "Select query" and
  "Provide SQL"

#### Scenario: Switching tabs preserves runtime input values

- **WHEN** the user has entered runtime parameter values, an output
  format and a row limit, and switches between the "Select query" and
  "Provide SQL" tabs
- **THEN** the runtime parameter values, output format and row limit
  SHALL be retained

### Requirement: Stored SQLQuery Library selection

In the "Select query" tab, the form SHALL display a `Select.Root`
populated with stored Library resources whose
`type.coding[].code = sql-query` (queried from the FHIR server). When
the user selects a Library, the form SHALL display:

- a read-only `TextArea` showing the Base64-decoded SQL from
  `Library.content[0].data`, with a copy-to-clipboard `IconButton`
  positioned over the top-right corner;
- a read-only summary listing each `relatedArtifact` (label and
  ViewDefinition reference);
- a read-only summary listing each declared parameter (name and FHIR
  type from `Library.parameter[]`);
- the runtime parameter values section (one input per declared
  parameter).

#### Scenario: Library list is empty

- **WHEN** the FHIR server has no Library resources matching the
  SQLQuery type code
- **THEN** the `Select.Root` SHALL not render
- **AND** the form SHALL display guidance prompting the user to use the
  "Provide SQL" tab

#### Scenario: User selects a stored Library

- **WHEN** the user selects a Library from the picker
- **THEN** the SQL preview SHALL display the decoded SQL text
- **AND** the related-artifact summary SHALL list each artefact's label
  and ViewDefinition reference
- **AND** the declared-parameters summary SHALL list each parameter's
  name and type
- **AND** the runtime parameter values section SHALL render an input
  per declared parameter

#### Scenario: User copies the previewed SQL

- **WHEN** the user clicks the copy `IconButton` over the SQL preview
- **THEN** the decoded SQL text SHALL be written to the system
  clipboard

### Requirement: Inline SQLQuery Library authoring

In the "Provide SQL" tab, the form SHALL allow the user to author a
SQLQuery Library inline, comprising:

- a `TextArea` for the SQL text rendered with a monospace font;
- a "Tables" section listing related-artifact rows; each row contains a
  label `TextField`, a `Select.Root` of stored ViewDefinitions, and a
  remove `IconButton`. An "Add table" button appends rows;
- a "Parameters" section listing declared-parameter rows; each row
  contains a name `TextField`, a type `Select.Root` (the supported FHIR
  primitives: string, integer, decimal, boolean, date, dateTime, code),
  an optional default `TextField`, and a remove `IconButton`. An "Add
  parameter" button appends rows;
- the runtime parameter values section (one input per declared
  parameter).

#### Scenario: Adding a table row

- **WHEN** the user clicks "Add table"
- **THEN** a new row SHALL be appended with empty label and unselected
  ViewDefinition

#### Scenario: Removing a table row

- **WHEN** the user clicks the remove `IconButton` on a table row
- **THEN** the row SHALL be removed from the form

#### Scenario: Adding a parameter row updates runtime inputs

- **WHEN** the user adds a parameter row with name `patient_id` and
  type `string`
- **THEN** the runtime parameter values section SHALL render an input
  for `patient_id` typed as string

#### Scenario: Renaming a declared parameter

- **WHEN** the user changes the name of a declared parameter
- **THEN** the runtime parameter values section SHALL render an input
  with the new name
- **AND** any value previously entered for the old name SHALL be
  discarded

### Requirement: Runtime parameter input typing

Each runtime parameter input SHALL match the declared parameter type:

- string and code SHALL render a `TextField`;
- integer SHALL render a numeric `TextField` accepting integers only;
- decimal SHALL render a numeric `TextField` accepting decimal values;
- boolean SHALL render a `Switch`;
- date SHALL render a date `TextField` (ISO 8601 date);
- dateTime SHALL render a dateTime `TextField` (ISO 8601 dateTime).

The user SHALL be able to leave runtime values empty, in which case the
parameter SHALL be omitted from the request's nested `parameters`
Parameters resource.

#### Scenario: Boolean parameter renders a switch

- **WHEN** a parameter is declared with type `boolean`
- **THEN** the runtime input SHALL render as a `Switch` defaulting to
  off
- **AND** the bound value submitted on the request SHALL be a
  `valueBoolean`

#### Scenario: Integer parameter rejects non-integer input

- **WHEN** the user types a non-integer value into an integer
  parameter input
- **THEN** the form SHALL not allow execution
- **AND** SHALL display field-level guidance indicating the expected
  type

### Requirement: Output format and execution options

The SQL query form SHALL provide:

- a `Select.Root` for the output format with values `csv`, `ndjson`,
  `json`, `parquet` and `fhir`;
- a numeric `TextField` for `_limit`;
- a `Switch` for `_header`, visible only when the output format is
  `csv`.

#### Scenario: Switching output format to non-csv hides the header switch

- **WHEN** the user changes the output format from `csv` to any other
  value
- **THEN** the `_header` `Switch` SHALL no longer render

#### Scenario: Empty limit submits without a row cap

- **WHEN** the user leaves the `_limit` field empty and clicks Execute
- **THEN** the request SHALL omit the `_limit` parameter

### Requirement: Execute action submits a $sqlquery-run request

When the user clicks the Execute button, the form SHALL submit a POST
to the FHIR server's `$sqlquery-run` operation with a Parameters body
containing:

- exactly one of `queryReference` (in "Select query" tab) or
  `queryResource` (in "Provide SQL" tab);
- the `_format` parameter when an output format is selected;
- the `_limit` parameter when a numeric limit is provided;
- the `_header` parameter when output format is `csv`;
- a `parameters` parameter holding a Parameters resource that lists
  one entry per declared parameter with a non-empty runtime value,
  using the `value[x]` slot matching the declared FHIR type.

The Execute button SHALL be disabled while a request is in flight or
when required inputs are missing (no Library selected in the "Select
Library" tab, or empty SQL or zero tables in the "Provide SQL"
tab).

#### Scenario: Execute is disabled when SQL is empty in inline mode

- **WHEN** the user is in the "Provide SQL" tab with an empty SQL
  text area
- **THEN** the Execute button SHALL be disabled

#### Scenario: Execute is disabled when no Library is selected in stored mode

- **WHEN** the user is in the "Select query" tab and has not
  selected a Library
- **THEN** the Execute button SHALL be disabled

#### Scenario: Execute submits queryResource for an inline Library

- **WHEN** the user clicks Execute in the "Provide SQL" tab with
  a non-empty SQL, at least one table row and a runtime value for each
  declared parameter
- **THEN** the form SHALL POST to `/$sqlquery-run` with a Parameters
  body containing exactly one `queryResource` parameter whose
  resource is a Library conforming to the SQLQuery profile, the
  selected output format under `_format`, and a `parameters`
  Parameters resource with the runtime bindings

#### Scenario: Execute submits queryReference for a stored Library

- **WHEN** the user clicks Execute in the "Select query" tab with a
  Library selected
- **THEN** the form SHALL POST to `/$sqlquery-run` with a Parameters
  body containing exactly one `queryReference` parameter whose value
  is a relative reference of the form `Library/<id>`

### Requirement: Save inline SQLQuery Library to server

In the "Provide SQL" tab, the form SHALL display a "Save to server"
button alongside the Execute button. Clicking it SHALL POST the
assembled SQLQuery Library to `/Library` and, on success, switch the
form to the "Select query" tab with the new Library's ID
pre-selected. On failure, the form SHALL display the server-returned
error message in a `Callout`. The button SHALL be hidden in the
"Select query" tab.

#### Scenario: Successful save switches to stored mode

- **WHEN** the user clicks "Save to server" in the "Provide SQL"
  tab and the server responds with `201 Created`
- **THEN** the form SHALL switch to the "Select query" tab
- **AND** the new Library's ID SHALL be the selected value in the
  Library picker

#### Scenario: Failed save surfaces server error

- **WHEN** the user clicks "Save to server" and the server responds
  with a 4xx or 5xx status
- **THEN** the form SHALL display a red `Callout` containing the
  server error message
- **AND** the form SHALL remain in the "Provide SQL" tab with the
  user's input intact

### Requirement: SQL query result card

Each Execute click SHALL append a result card to the right-hand result
column. The card SHALL be implemented as a Radix Themes `Card` and
SHALL include:

- a header with the title "SQL query", the requested output format as
  a `Badge`, and a job ID and timestamp;
- a close `Button` once the request has terminated (success or
  failure);
- a body that depends on the response:
    - while the request is pending, a `Spinner` and "Executing SQL
      query…" message;
    - on success with a tabular format (`csv`, `ndjson`, `json`,
      `fhir`), a `Table` listing parsed rows, a row count `Badge` and a
      download button that streams the original response body;
    - on success with `parquet`, a download button only;
    - on failure, a red `Callout` with the server error message and the
      submitted SQL displayed in a monospace block above it.

Multiple result cards SHALL be supported in the result column. The
most recent SHALL appear at the top.

#### Scenario: Pending request shows a spinner

- **WHEN** the request is in flight
- **THEN** the result card body SHALL display a spinner and the message
  "Executing SQL query…"

#### Scenario: Successful CSV response renders a table

- **WHEN** the request succeeds with a `text/csv` response body
- **THEN** the card body SHALL render a `Table` with one column per
  CSV column and one row per data row
- **AND** SHALL display a row count badge
- **AND** SHALL display a download button labelled "Download .csv"

#### Scenario: Successful FHIR-format response is flattened

- **WHEN** the request succeeds with `_format=fhir` and an
  `application/fhir+json` Parameters resource as the body
- **THEN** the card body SHALL render a `Table` whose columns are the
  unique `part.name` values across all `row` parameters
- **AND** the cells SHALL display the part's `value[x]` rendered as a
  string

#### Scenario: Successful parquet response is download-only

- **WHEN** the request succeeds with `_format=parquet`
- **THEN** the card body SHALL not render a `Table`
- **AND** SHALL render a download button labelled "Download .parquet"

#### Scenario: Failed response surfaces the OperationOutcome

- **WHEN** the server responds with a 4xx or 5xx status and an
  `OperationOutcome` body
- **THEN** the card body SHALL display the submitted SQL in a monospace
  block
- **AND** the issue's diagnostics or details text SHALL be displayed
  inside a red `Callout`

#### Scenario: User closes a result card

- **WHEN** the user clicks the close button on a terminated result
  card
- **THEN** the card SHALL be removed from the result column
- **AND** other result cards SHALL be unaffected

### Requirement: SQL query API client and TanStack Query hooks

A new `sqlQuery.ts` module under `ui/src/api/` SHALL expose typed
functions for invoking `$sqlquery-run` (returning a streamed response)
and for listing Library resources whose
`type.coding[].code = sql-query`. New TanStack Query hooks SHALL wrap
these functions:

- `useSqlQueryRun` SHALL provide an imperative `execute` function and
  expose status, result and error values, mirroring the API of
  `useViewRun`.
- `useSqlQueryLibraries` SHALL fetch and cache the list of stored
  SQLQuery Libraries.
- `useSaveSqlQueryLibrary` SHALL POST a Library and return the
  created resource's ID, mirroring `useSaveViewDefinition`.

#### Scenario: useSqlQueryRun parses CSV responses into rows

- **WHEN** the form invokes `useSqlQueryRun` with `_format=csv` and the
  server returns CSV text
- **THEN** the hook SHALL resolve a result with a columns array and a
  rows array parsed from the CSV body

#### Scenario: useSqlQueryLibraries scopes the search by type code

- **WHEN** `useSqlQueryLibraries` runs
- **THEN** it SHALL issue a GET to `/Library` with a `type` search
  parameter constraining the value to the canonical SQLQuery type
  coding system and code `sql-query`

#### Scenario: useSqlQueryRun returns a stream for parquet

- **WHEN** the form invokes `useSqlQueryRun` with `_format=parquet`
- **THEN** the hook SHALL resolve with a binary blob suitable for
  download instead of attempting to parse rows
