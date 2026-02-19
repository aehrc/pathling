## Why

The bulk export `_typeFilter` parameter allows users to filter exported resources
using standard FHIR search parameters (e.g. `Patient?active=true`). The server
already has a spec for this (`bulk-export-type-filter`), but the admin UI export
form hides the type filter input behind an unused `showExtendedOptions` flag, and
the existing implementation is just a raw text field. The Resources page already
has a well-designed search parameters component with dropdown-based parameter
selection and type badges. Extracting and reusing this component would give users
a guided, structured way to build type filters in the export tab.

## What Changes

- Extract the search parameter row UI (parameter name dropdown + value input +
  add/remove controls) from `ResourceSearchForm` into a reusable
  `SearchParamsInput` component.
- Refactor `ResourceSearchForm` to use the extracted component.
- Add a type filters section to `ExportForm` that pairs a resource type selector
  with the extracted search parameters component, allowing users to build
  `_typeFilter` expressions per resource type.
- Wire `typeFilters` through `ExportRequest`, `BulkExportRequest`, and
  `BulkExportBaseOptions` to the bulk export API kick-off functions as
  `_typeFilter` query parameters.
- Remove the hidden raw text field for type filters from `ExportOptions`.

## Capabilities

### New Capabilities

- `export-type-filter-ui`: UI support for building and submitting `_typeFilter`
  parameters in the export form, including a reusable search parameters input
  component extracted from the resources page.

### Modified Capabilities

- `search-parameter-form`: The search parameter row UI is extracted into a
  standalone reusable component; the resources page is refactored to consume it.

## Impact

- `ui/src/components/resources/ResourceSearchForm.tsx` — refactored to use
  extracted component.
- `ui/src/components/export/ExportForm.tsx` — gains type filter section.
- `ui/src/components/export/ExportOptions.tsx` — raw type filter text field
  removed.
- `ui/src/types/export.ts` — `ExportRequest` gains `typeFilters` field.
- `ui/src/hooks/useBulkExport.ts` — `BulkExportRequest` gains `typeFilters`.
- `ui/src/api/bulkExport.ts` — `BulkExportBaseOptions` gains `typeFilters`;
  kick-off functions send `_typeFilter` query params.
- New shared component file(s) for the extracted search parameters input.
