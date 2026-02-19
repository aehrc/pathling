## 1. Update types and defaults

- [x] 1.1 Move `TypeFilterState` into `exportOptions.ts` and add `typeFilters: TypeFilterState[]` to `ExportOptionsValues`
- [x] 1.2 Update `DEFAULT_EXPORT_OPTIONS` to include `typeFilters: []`
- [x] 1.3 Move `serialiseTypeFilters` from `ExportForm.tsx` to `exportOptions.ts`

## 2. Update ExportOptions component

- [x] 2.1 Add `searchParams` optional prop to `ExportOptions`
- [x] 2.2 Add type filter add/remove/update handlers inside `ExportOptions`
- [x] 2.3 Render type filter section (heading, help text, add button, filter entry cards with `SearchParamsInput`)

## 3. Simplify ExportForm

- [x] 3.1 Remove `TypeFilterState` type, `typeFilters` state, and all type filter handlers from `ExportForm`
- [x] 3.2 Remove type filter JSX from `ExportForm`
- [x] 3.3 Pass `searchParams` prop through to `ExportOptions`
- [x] 3.4 Update `handleSubmit` to read type filters from `exportOptions.typeFilters` via imported `serialiseTypeFilters`

## 4. Wire up ImportPnpForm

- [x] 4.1 Add `searchParams` prop to `ImportPnpForm` and pass through to `ExportOptions`
- [x] 4.2 Update `ImportPnpForm.handleSubmit` to serialise `exportOptions.typeFilters` and include in `ImportPnpRequest`
- [x] 4.3 Update the parent that renders `ImportPnpForm` to pass `searchParams`

## 5. Update tests

- [x] 5.1 Update type filter tests to interact via `ExportOptions` rather than `ExportForm` internals
- [x] 5.2 Add test for ImportPnpForm submitting type filters
- [x] 5.3 Verify existing export form tests still pass
