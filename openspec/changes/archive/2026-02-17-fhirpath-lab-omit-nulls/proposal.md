## Why

Complex type results from the FHIRPath Lab API include null-valued fields (e.g.
`"id": null`, `"comparator": null`), adding noise to the JSON output. Omitting
these fields produces cleaner, more readable responses without losing
information.

## What Changes

- Strip null-valued keys from complex type result dicts before JSON
  serialisation.
- Apply the stripping recursively so nested structs are also cleaned.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `fhirpath-lab-server`: Complex type result formatting requirement updated to
  exclude null-valued fields from the JSON representation.

## Impact

- `fhirpath-lab-api/src/fhirpath_lab_api/parameters.py` - result serialisation
  logic.
- No API contract changes (null fields carry no information).
