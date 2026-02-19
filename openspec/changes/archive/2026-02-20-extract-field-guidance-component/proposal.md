## Why

Field guidance text (small grey helper text below form fields) is repeated inline across ~10 components using the same `<Text size="1" color="gray" mt="1">` pattern. Extracting a shared `FieldGuidance` component eliminates this duplication and provides a single point of control for styling and semantics.

## What changes

- Introduce a `FieldGuidance` component that renders field helper text with consistent styling.
- Replace all inline `<Text size="1" color="gray" ...>` guidance text instances with the new component.

## Capabilities

### New capabilities

- `field-guidance`: A reusable component for rendering field-level guidance text below form inputs.

### Modified capabilities

(None - this is a pure refactoring with no requirement changes.)

## Impact

- **Components affected**: `ExportOptions`, `SearchParamsInput`, `ResourceSearchForm`, `ImportForm`, `ImportPnpForm`, `SqlOnFhirForm`, `ResourceTypePicker`.
- **No API or behavioural changes** - purely visual/structural refactoring.
- **No new dependencies** - uses existing Radix UI `Text` component internally.
