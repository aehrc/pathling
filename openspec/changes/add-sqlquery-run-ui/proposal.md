## Why

The Pathling server now implements the SQL on FHIR `$sqlquery-run` operation,
which lets users execute ad-hoc SQL against ViewDefinition tables via a
SQLQuery Library resource. The admin UI currently only exposes
`$viewdefinition-run`, so there is no interactive way to author SQL, bind
runtime parameters, pick output formats and inspect results. Adding this to
the SQL on FHIR page closes the gap between the server capability and the
operator experience, and gives users a place to develop and test SQL
queries against their FHIR data without leaving the admin UI.

## What Changes

- Add a new "SQL query" mode to the existing SQL on FHIR page so the user can
  choose between running a ViewDefinition (`$viewdefinition-run`) and running
  a SQL query (`$sqlquery-run`).
- In SQL query mode, allow the user to:
    - Either select a stored Library that conforms to the SQLQuery profile, or
      author an inline query.
    - Edit the SQL text in a monospace text area (round-tripped through Base64
      in the Library `content`).
    - Pick depended-on ViewDefinitions from the server and assign each a
      `label` (the table name used in the SQL), backed by `relatedArtifact`.
    - Declare and bind runtime parameters (name, FHIR type, value), submitted
      as the `parameters` Parameters resource.
    - Choose an output format (`ndjson`, `csv`, `json`, `parquet`, `fhir`) and
      optional row limit / CSV header toggle.
- Execute the request and render the result in a card alongside the existing
  ViewDefinition result cards, with the same close / multi-card behaviour.
  Tabular formats render in a `Table`; the `fhir` Parameters response is
  flattened into rows; binary formats (`parquet`) offer a download.
- Surface validation errors from the server (e.g. disallowed SQL operations,
  missing parameter bindings, unresolved Library) in a Callout.
- Add a "Save to server" action in inline mode that POSTs the assembled
  SQLQuery Library and switches the form to "stored" mode pointing at the
  new resource, mirroring the existing ViewDefinition flow.
- Add a new client-side API module and TanStack Query hook for invoking
  `$sqlquery-run`, plus a hook for listing stored SQLQuery Libraries
  (Library resources with `type.coding.code = sql-query`).
- The existing `SqlOnFhirForm` is generalised to host both modes via a top
  segmented control; the ViewDefinition flow keeps its existing behaviour.

## Capabilities

### New Capabilities

- `sqlquery-run-ui`: UI support for authoring, parameterising, executing and
  inspecting SQL queries through the server's `$sqlquery-run` operation,
  including stored-Library selection, inline-Library authoring, runtime
  parameter binding, output-format selection and result rendering.

### Modified Capabilities

(none - the existing SQL on FHIR ViewDefinition page does not have a
dedicated capability spec; new behaviour is wholly contained in the new
capability.)

## Impact

- `ui/src/pages/SqlOnFhir.tsx` - hosts the new mode toggle and a second
  result-card type alongside the existing `ViewCard`.
- `ui/src/components/sqlOnFhir/` - new components for the SQL query form,
  related-artifact picker, parameter editor and result card.
- `ui/src/api/` - new `sqlQuery.ts` module for `$sqlquery-run` and listing
  SQLQuery Libraries.
- `ui/src/hooks/` - new `useSqlQueryRun.ts` and `useSqlQueryLibraries.ts`
  hooks; a new `useSaveSqlQueryLibrary.ts` mirroring
  `useSaveViewDefinition.ts`.
- `ui/src/types/` - new `sqlQuery.ts` type module covering the SQLQuery
  Library shape, parameter declarations and request/result types.
- No server changes: the operation, profile and output formats already
  exist on `feature/sql-run-operation`.
- No new runtime dependencies; the form is built with the existing Radix
  Themes / Radix Primitives stack already used by the admin UI.
