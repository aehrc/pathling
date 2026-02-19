## Why

The type filter UI (resource type selector, search parameter rows, add/remove
controls) is currently implemented inline within `ExportForm`. Moving it into
`ExportOptions` consolidates all export configuration fields in one reusable
component, reducing duplication if type filters are needed in other export
contexts (e.g. export-by-query) and simplifying `ExportForm`.

## What Changes

- Add type filter state and UI to `ExportOptions`, including the
  `TypeFilterState` type, add/remove/update handlers, and the rendered card list
  with `SearchParamsInput`.
- Extend `ExportOptionsValues` with a `typeFilters` field so type filter state
  flows through the same `values`/`onChange` interface as other export options.
- Add `searchParams` prop to `ExportOptions` so it can populate search parameter
  dropdowns per resource type.
- Remove all type filter state, handlers, and JSX from `ExportForm`, delegating
  to `ExportOptions` instead.
- Update `ExportForm.handleSubmit` to read type filters from
  `exportOptions.typeFilters` rather than separate state.
- Update `ImportPnpForm` to pass `searchParams` to `ExportOptions` and
  serialise type filters from `exportOptions.typeFilters` into the
  `ImportPnpRequest`. The hook and API layer already support `typeFilters`.
- Update tests to reflect the new component boundaries.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `export-type-filter-ui`: Type filter UI moves from `ExportForm` into
  `ExportOptions`; all existing requirements remain unchanged but the component
  boundary shifts. `ImportPnpForm` gains type filter support via
  `ExportOptions`.

## Impact

- `ui/src/components/export/ExportOptions.tsx` - gains type filter UI and
  `searchParams` prop.
- `ui/src/types/exportOptions.ts` - `ExportOptionsValues` gains `typeFilters`
  field; `DEFAULT_EXPORT_OPTIONS` updated.
- `ui/src/components/export/ExportForm.tsx` - type filter state and UI removed;
  reads from `ExportOptions` values.
- `ui/src/components/import/ImportPnpForm.tsx` - passes `searchParams` to
  `ExportOptions`; `handleSubmit` serialises and includes type filters in
  request.
- Existing tests in `ExportFormTypeFilters.test.tsx` and any ExportOptions tests
  will need updating.
