## Context

The admin UI exposes ViewDefinition execution at `/sql-on-fhir`. The page
follows a familiar pattern across the admin UI: a left-hand `Card` form
that triggers an action, and a right-hand column of result cards
(`ViewCard`), one per submitted job, each managing its own lifecycle
through TanStack Query.

The server now offers a second SQL on FHIR operation - `$sqlquery-run` -
which takes a Library resource conforming to the `SQLQuery` profile. The
Library wraps Base64-encoded SQL, references one or more ViewDefinitions
via `relatedArtifact` (whose `label` becomes the table name in the SQL),
declares typed runtime parameters, and supports five output formats
(`csv`, `ndjson`, `json`, `parquet`, `fhir`). The HTTP request supplies
the Library either inline (`queryResource`) or by reference
(`queryReference`) and binds runtime parameters via a nested
`parameters` Parameters resource.

The `feature/sql-run-operation` branch already implements the operation
on the server, so this change is wholly UI-side. The user has asked for
wireframes (lo-fi HTML wireframes co-located with the change) and
adherence to the project's Radix Themes conventions.

## Goals / Non-Goals

**Goals:**

- Add a "SQL query" mode to the existing SQL on FHIR page using a top
  segmented control, sharing the same page chrome and authentication
  guards as the ViewDefinition flow.
- Let users author a SQLQuery Library inline (SQL text + related
  ViewDefinitions + parameter declarations), or pick a stored Library
  conforming to the SQLQuery profile.
- Render results in a `Table` for tabular formats (`csv`, `ndjson`,
  `json`, `fhir`) and offer a download for `parquet`.
- Surface server-side validation errors (e.g. disallowed SQL operations,
  missing parameter bindings) in a `Callout`.
- Provide a "Save to server" path that POSTs the assembled Library and
  flips to "stored" mode pointing at the new resource, mirroring the
  ViewDefinition flow.

**Non-Goals:**

- Async export of SQL query results. The operation is synchronous-only;
  there is no `$sqlquery-export` to mirror `$viewdefinition-export`.
- A SQL syntax-highlighting editor. We keep the existing `TextArea`
  monospace pattern - sufficient at this stage and consistent with the
  ViewDefinition JSON editor.
- Editing or deleting stored SQLQuery Libraries (the existing UI does
  not offer this for ViewDefinitions either).
- Server-side changes. The operation, profile and output formats are
  already implemented on `feature/sql-run-operation`.
- Authoring the `parameter` definitions on a stored Library; the user
  binds against parameters declared by an existing Library, but inline
  Libraries declare parameters in the same form.

## Decisions

### Single page, mode-toggled form

The SQL on FHIR page hosts both `$viewdefinition-run` and
`$sqlquery-run` behind a top-level `Tabs.Root` ("View definition" vs
"SQL query") above the form card. Switching tabs swaps the form body
but keeps the result column visible, so cards from either mode can
coexist.

**Rationale:** Both operations are SQL on FHIR primitives that operate
on ViewDefinitions; users will naturally compare results. A single
page is simpler than two routes, avoids navigation friction during query
development, and keeps the existing left-form/right-results layout
intact. The Import page already uses a top-level `Tabs.Root` to switch
between "Import from URLs" and "Import from FHIR server"; reusing that
pattern keeps page-level mode switches visually consistent across the
admin UI.

**Alternatives considered:**

- Separate `/sql-query` route - rejected because it duplicates the auth
  guard, capabilities check and result column wiring without a
  meaningful UX gain.
- A `SegmentedControl` for the outer mode - inconsistent with the rest
  of the admin UI, which uses tabs at the page level (e.g. the Import
  page). The inner ViewDefinition form already uses `Tabs.Root` for
  "Select view definition" / "Provide JSON"; nesting tabs inside tabs
  is acceptable here because the inner tabs sit inside their own card
  with distinct styling.

### Form structure in SQL query mode

The SQL query form uses the same outer `Card` as the ViewDefinition
form, with the following sections (top to bottom):

1. `Tabs.Root` to switch between "Select query" (stored) and
   "Provide SQL" (inline) - mirrors the ViewDefinition flow.
2. **Stored tab:** `Select.Root` listing Libraries with
   `type.coding.code = sql-query`. Below the picker, a read-only
   `TextArea` previews the decoded SQL with a copy-to-clipboard
   icon, plus a read-only summary of `relatedArtifact` and declared
   parameters.
3. **Inline tab:**
    - SQL `TextArea` (monospace, ~16 rows) for editing the query.
    - "Tables" section: a list of related-artifact rows. Each row is a
      `label` `TextField` (the table name used in the SQL) paired with
      a `Select.Root` listing stored ViewDefinitions; an "Add table"
      button appends rows and a `TrashIcon` `IconButton` removes them.
    - "Parameters" section: a list of declared-parameter rows
      (`name` `TextField`, `type` `Select.Root` from the FHIR primitive
      types we support, `default` `TextField`); same add/remove pattern.
4. **Common, below tabs:**
    - "Runtime parameter values" section: one row per declared parameter
      (whether the Library is stored or inline), with a typed input
      appropriate to the declared FHIR type (string, integer, decimal,
      boolean, date, dateTime, code).
    - "Output format" `Select.Root` (csv / ndjson / json / parquet /
      fhir) and a `_limit` `TextField` (numeric). When the format is
      `csv`, a `_header` `Switch` appears.
5. Action row: "Execute" (primary) and, in inline mode only, "Save to
   server" (soft variant), matching the ViewDefinition form.

**Rationale:** Reusing the existing two-tab pattern keeps the page
visually coherent. Pulling the runtime parameter values out of the
"inline" tab matches the server's request shape (parameter declarations
live on the Library; runtime bindings live on the request) and means
the same controls work for stored and inline queries.

### Library construction and persistence

Inline SQLQuery Libraries are assembled client-side from the form
state and shaped to match the SQLQuery profile:

- `resourceType: "Library"`, `status: "active"`,
  `meta.profile: ["https://sql-on-fhir.org/ig/StructureDefinition/SQLQuery"]`.
- `type.coding[0]`:
  `{system: ".../LibraryTypesCodes", code: "sql-query"}`.
- `content[0]`:
  `{contentType: "application/sql", data: <Base64(SQL)>}`. We add the
  `sql-text` extension carrying the plain SQL too, matching the
  examples in the operation definition.
- `relatedArtifact`: `{type: "depends-on", label, resource: "ViewDefinition/<id>"}`
  per row.
- `parameter`: declared parameters mapped to FHIR
  `ParameterDefinition` (`name`, `use: "in"`, `type`, optional default
  via `extension` if needed).

The "Save to server" button POSTs this Library to `/Library` (mirroring
`useSaveViewDefinition`) and switches to the stored tab, pre-selecting
the new ID. Stored Libraries are listed via
`/Library?type=https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes|sql-query`
through a new `useSqlQueryLibraries` hook that mirrors
`useViewDefinitions`.

### Runtime parameter binding

Runtime parameter values are submitted as a `parameters` Parameters
resource nested in the request:

```json
{
    "name": "parameters",
    "resource": {
        "resourceType": "Parameters",
        "parameter": [{ "name": "patient_id", "valueString": "Patient/123" }]
    }
}
```

The form maps each declared parameter's FHIR type to a typed input and
to the appropriate `value[x]` slot at submission time. We do **not**
support the alternative repeated-`parameter`-with-parts form shown in
the operation definition examples; the nested-Parameters form is
unambiguously typed and matches the canonical example.

### Result rendering

Result rendering branches on the requested output format:

- `csv` / `ndjson` / `json`: parsed into rows + columns and rendered
  in `Table` (reusing the same NDJSON parsing utilities as
  `useViewRun`; CSV uses a small new parser; JSON is a single array).
- `fhir`: the response is a Parameters resource where each `row`
  parameter has typed `part`s. We flatten each row into a
  `{column: stringified value}` map and render the same `Table`,
  adding a "FHIR types" badge on each column header to distinguish it
  from the binary formats.
- `parquet`: binary - we render a download button instead of a table,
  with a row showing file size and timestamp.

This logic lives in a new `SqlQueryCard` component, parallel to
`ViewCard`. Both consume the existing close-button / multi-card
pattern in `SqlOnFhir.tsx`.

### Error handling

The server returns `OperationOutcome` for invalid input (SQL
validation, missing bindings, unresolved Library, etc.). Error messages
are surfaced in a red `Callout.Root` inside the result card. We do not
attempt client-side SQL validation - the server's reject-list is
authoritative and would be brittle to mirror.

### State, not effects, for derived data

The runtime-parameter input list is derived from the active Library's
declared parameters: when the user picks a stored Library, we read its
`parameter` array; when they edit the inline declarations, the runtime
inputs follow. This is computed during render from the form state, not
synced via an effect, in line with the project React rules.

### Wireframes

Lo-fi HTML wireframes are produced via the wireframing skill and
written to `wireframes/` under this change directory. They cover:

1. SQL on FHIR page - SQL query mode, "Provide SQL" tab,
   parameters declared, runtime values bound, ready to execute.
2. SQL on FHIR page - SQL query mode, "Select query" tab, with a
   stored Library selected.
3. Result card - tabular result rendered after execution, with a
   format badge and download (when applicable).
4. Result card - error state showing an `OperationOutcome` callout.

## Risks / Trade-offs

- **Form complexity:** the inline form mixes SQL, related artefacts and
  parameter declarations. → Mitigation: keep each section in its own
  `Box` with a `FieldLabel` and `FieldGuidance`, and lean on the
  existing `Tabs` to keep stored vs inline visually separated.
- **Output-format result divergence:** different formats produce
  different shapes (text vs Parameters vs binary). → Mitigation:
  branch on format in `SqlQueryCard` and gate the table on
  parsability; binary formats fall back to a download.
- **No syntax highlighting:** plain `TextArea` for SQL is uglier than a
  proper editor. → Mitigation: deferred; see Non-Goals. We already
  ship JSON in a plain `TextArea` for ViewDefinitions and have not
  hit pushback.
- **FHIR-format flattening:** turning typed `part`s into strings drops
  type info from the rendered table. → Mitigation: include a
  per-column type indicator above the table; users who need exact
  types can switch to `_format=ndjson` or download `parquet`.
- **Library round-trip on save:** assembling a Library client-side
  risks producing an invalid resource that the server rejects on
  save. → Mitigation: surface server validation errors in the same
  Callout as the execute path, and (a) require a non-empty SQL,
  (b) require at least one related artefact, (c) require a label per
  related artefact in the form before enabling the save button.

## Open Questions

- Should "Save to server" require a `name` and `url` on the Library, or
  should we let the server reject incomplete saves? Leaning towards a
  required `name` field for usability and storing it as
  `Library.name`, with `url` left optional.
- Should the parameter editor support FHIR `Quantity` and `Coding`
  declared types? The server's parser handles them; the UI would need
  composite inputs. Initial scope: primitives only (string, integer,
  decimal, boolean, date, dateTime, code), with a follow-up issue if
  users need composites.
