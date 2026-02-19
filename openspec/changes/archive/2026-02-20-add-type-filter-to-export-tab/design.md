## Context

The export tab currently accepts resource type selection, date filters, elements,
and output format. The `_typeFilter` bulk export parameter allows per-resource-type
search filtering (e.g. `Patient?active=true`), but the UI only has a hidden raw
text field for this.

The Resources page already has a search parameters component within
`ResourceSearchForm` that renders parameter dropdown + value rows using data
from the CapabilityStatement. This component is currently embedded in the form
and cannot be reused elsewhere.

## Goals / Non-Goals

**Goals:**

- Extract the search parameter rows into a reusable `SearchParamsInput` component
  that can be consumed by both the resources page and the export form.
- Add a "Type filters" section to `ExportForm` where users can add type filter
  entries, each consisting of a resource type selector paired with a
  `SearchParamsInput` for that type's search parameters.
- Wire the type filter data through to the bulk export API as `_typeFilter` query
  parameters.

**Non-Goals:**

- Server-side `_typeFilter` parsing and filtering (already covered by the
  `bulk-export-type-filter` spec and a separate implementation effort).
- Supporting `includeAssociatedData` in the UI.
- Changing the search parameters behaviour on the Resources page (beyond
  refactoring the component extraction).

## Decisions

### Extract SearchParamsInput as a shared component

The search parameter rows (dropdown + value + add/remove) will be extracted into
`ui/src/components/SearchParamsInput.tsx`. This component will accept:

- `availableParams: SearchParamCapability[]` — the parameters to show in the
  dropdown.
- `rows: SearchParamRowData[]` — the current row state.
- `onChange: (rows: SearchParamRowData[]) => void` — callback when rows change.

The component will own the "Add parameter" button, row rendering, and help text.
`ResourceSearchForm` will supply `availableParams` derived from the selected
resource type and manage the row state externally.

**Rationale:** Lifting state management (rows) to the parent keeps the component
stateless and composable. The export form needs to manage multiple independent
`SearchParamsInput` instances (one per type filter entry), each with separate
row state.

### Type filter section in ExportForm

Each type filter entry in the export form will be a card-like row containing:

1. A resource type dropdown (populated from the same list as the resource type
   picker).
2. A `SearchParamsInput` for that resource type's search parameters.
3. A remove button.

An "Add type filter" button will append a new entry. The section will appear
between `ExportOptions` and the submit button, and will be labelled "Type
filters". Help text will explain the `_typeFilter` semantics.

**Rationale:** This approach mirrors the `ResourceType?search-params` format of
`_typeFilter` directly in the UI. Each entry maps to one `_typeFilter` value.

### Type filter data model

Type filters will be stored as an array on `ExportRequest`:

```typescript
typeFilters?: Array<{
  resourceType: string;
  params: Record<string, string[]>;
}>
```

At submission, each entry will be serialised to the `_typeFilter` query format:
`ResourceType?param1=value1&param2=value2`. Multiple entries for the same
resource type will produce multiple `_typeFilter` values (OR logic on the
server).

### Remove typeFilters from ExportOptionsValues

The raw `typeFilters` string field will be removed from `ExportOptionsValues` and
`ExportOptions` since the new structured UI replaces it entirely. The
`showExtendedOptions` prop and the `includeAssociatedData` field will also be
removed since they have no consumers.

### API layer changes

`BulkExportBaseOptions` will gain a `typeFilters?: string[]` field containing
pre-serialised `_typeFilter` strings. `buildExportParams` will be updated to
produce `_typeFilter` query parameters. Since `_typeFilter` can appear multiple
times, `buildExportParams` will return `URLSearchParams` instead of
`Record<string, string>` to support repeated keys.

## Risks / Trade-offs

- **CapabilityStatement required for search params in export:** The export form
  needs search parameter definitions from the CapabilityStatement to populate
  dropdowns. The `Export` page already fetches capabilities, so this data is
  available. If the server declares no search params for a resource type, the
  user can still add a type filter entry but the dropdown will be empty (matching
  the resources page behaviour). → Acceptable; users can always fall back to not
  using type filters.

- **Serialisation complexity:** Building `_typeFilter` strings from structured
  data must handle edge cases (empty values, multiple values for the same param).
  → Mitigated by reusing the same row-filtering logic already proven in
  `ResourceSearchForm` (skip rows with empty param name or value).
