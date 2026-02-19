## Why

Every form field in the UI renders its label with the same inline pattern:
`<Text as="label" size="2" weight="medium" mb="1">`. This is duplicated across
10+ files and ~20 instances. Extracting a `FieldLabel` component (mirroring the
recently extracted `FieldGuidance`) will reduce duplication, enforce consistency,
and make future styling changes a single-point edit.

## What changes

- New `FieldLabel` component that renders a label with standard typography
  (`size="2"`, `weight="medium"`) and an optional "(optional)" suffix.
- Replace all inline `<Text as="label" size="2" weight="medium" ...>` instances
  across form components with `FieldLabel`.

## Capabilities

### New capabilities

- `field-label`: A reusable `FieldLabel` component for rendering consistent form
  field labels, with support for an optional marker suffix.

### Modified capabilities

## Impact

- UI module only (`ui/src/components/`).
- Affected files: `ExportForm.tsx`, `ExportOptions.tsx`,
  `ResourceTypePicker.tsx`, `ImportForm.tsx`, `ImportPnpForm.tsx`,
  `SaveModeField.tsx`, `SqlOnFhirForm.tsx`, `ResourceSearchForm.tsx`,
  `SearchParamsInput.tsx`, `BulkSubmitMonitorForm.tsx`.
- No API, server, or dependency changes.
- Visual output must remain identical.
