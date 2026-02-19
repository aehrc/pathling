## 1. Extract reusable SearchParamsInput component

- [x] 1.1 Create `SearchParamsInput` component at
      `ui/src/components/SearchParamsInput.tsx` with props for `availableParams`,
      `rows`, `onChange`, and optional `onKeyDown`; extract the row rendering,
      add/remove logic, and help text from `ResourceSearchForm`
- [x] 1.2 Write tests for `SearchParamsInput` covering row rendering, add, remove,
      remove-disabled-on-last-row, parameter name change, and value change
- [x] 1.3 Refactor `ResourceSearchForm` to use `SearchParamsInput`, managing row
      state and passing `availableParams` derived from the selected resource type
- [x] 1.4 Verify existing `ResourceSearchForm` tests still pass

## 2. Clean up ExportOptions

- [x] 2.1 Remove `typeFilters` and `includeAssociatedData` from
      `ExportOptionsValues`, `DEFAULT_EXPORT_OPTIONS`, and remove the
      `showExtendedOptions` prop and its associated UI from `ExportOptions`
- [x] 2.2 Update any existing tests affected by the removed fields

## 3. Add type filter section to ExportForm

- [x] 3.1 Add `typeFilters` field to `ExportRequest` type as
      `Array<{ resourceType: string; params: Record<string, string[]> }>`
- [x] 3.2 Add type filter state management to `ExportForm` (array of entries, each
      with resource type, search param rows, and a unique ID)
- [x] 3.3 Render the type filters section in `ExportForm` with "Add type filter"
      button, per-entry resource type dropdown, `SearchParamsInput`, and remove button
- [x] 3.4 Pass `searchParams` (from CapabilityStatement) into `ExportForm` so
      dropdowns can be populated per resource type
- [x] 3.5 Serialise type filter entries into the `ExportRequest` on form submission
- [x] 3.6 Write tests for `ExportForm` type filter rendering, add/remove entries,
      resource type selection resetting params, and submission serialisation

## 4. Wire type filters through API layer

- [x] 4.1 Add `typeFilters?: string[]` to `BulkExportRequest` and
      `BulkExportBaseOptions`
- [x] 4.2 Update `buildExportParams` to emit `_typeFilter` query parameters,
      switching to `URLSearchParams` to support repeated keys
- [x] 4.3 Update `ExportCard` to serialise `ExportRequest.typeFilters` into
      `_typeFilter` strings and pass them to `useBulkExport`
- [x] 4.4 Write tests for `buildExportParams` with type filters and for the
      serialisation logic
