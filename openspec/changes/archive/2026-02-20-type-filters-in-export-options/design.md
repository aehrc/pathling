## Context

The `ExportOptions` component currently renders resource type selection, date
range filters, elements, and output format. Type filter UI (add/remove filter
entries, resource type dropdown per entry, search parameter rows) lives inline
in `ExportForm` with its own state management. This creates a split where some
export configuration lives in `ExportOptions` and some in `ExportForm`.

## Goals / Non-Goals

**Goals:**

- Consolidate all export configuration fields (including type filters) inside
  `ExportOptions`.
- Keep the existing type filter UX unchanged.
- Make type filters available to any consumer of `ExportOptions` without
  duplicating state management.

**Non-Goals:**

- Changing the type filter serialisation logic or API request format.
- Modifying `SearchParamsInput` behaviour.

## Decisions

### Extend ExportOptionsValues with typeFilters field

Add a `typeFilters: TypeFilterState[]` field to `ExportOptionsValues` and move
`TypeFilterState` into `exportOptions.ts`. This keeps all export config in a
single state object that flows through `values`/`onChange`.

Alternative considered: passing type filters as a separate prop/callback pair on
`ExportOptions`. Rejected because it breaks the single-values-object pattern
already established.

### Pass searchParams through ExportOptions

Add an optional `searchParams` prop to `ExportOptions` to provide per-resource-type
search parameter metadata. This is needed for the search parameter dropdowns
within type filter entries.

### Move serialiseTypeFilters to exportOptions.ts

Since `TypeFilterState` moves to `exportOptions.ts`, the serialisation function
that converts internal state to `TypeFilterEntry[]` should move there too,
keeping type and logic co-located. `ExportForm.handleSubmit` will import and
call it.

### Keep ExportForm as the state owner

`ExportForm` continues to own the `ExportOptionsValues` state via `useState`.
`ExportOptions` remains a controlled component receiving `values` and `onChange`.

### Wire up ImportPnpForm

`ImportPnpForm` already renders `ExportOptions` and its hook already supports
`typeFilters?: string[]`. The form just needs to: accept `searchParams`, pass it
through to `ExportOptions`, and serialise `exportOptions.typeFilters` into the
`ImportPnpRequest` on submit. No API or hook changes required.

## Risks / Trade-offs

- Slightly larger `ExportOptions` component, but the total code is the same -
  just relocated. Mitigated by the component already being a form-fields
  aggregator by design.
- `ExportOptions` gains a dependency on `SearchParamsInput`. This is acceptable
  since both are export-specific components in the same directory.
