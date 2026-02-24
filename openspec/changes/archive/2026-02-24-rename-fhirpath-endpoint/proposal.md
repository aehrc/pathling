## Why

The current endpoint path `/$fhirpath-r4` does not follow FHIR conventions. FHIR operations are typically served under a `/fhir` base path and use the `$` prefix for operation names without version suffixes. Renaming to `/fhir/$fhirpath` aligns with standard FHIR API patterns and removes the R4-specific suffix, making the API cleaner and more consistent.

## What changes

- **BREAKING**: The `POST /$fhirpath-r4` endpoint is renamed to `POST /fhir/$fhirpath`.
- All tests, documentation, and deployment configuration are updated to reflect the new path.

## Capabilities

### New capabilities

_(none)_

### Modified capabilities

- `fhirpath-lab-server`: The endpoint path changes from `/$fhirpath-r4` to `/fhir/$fhirpath`.

## Impact

- **Code**: `fhirpath-lab-api/src/fhirpath_lab_api/app.py` route definition and tests in `tests/test_app.py`.
- **Documentation**: `fhirpath-lab-api/README.md` and `openspec/specs/fhirpath-lab-server/spec.md`.
- **Clients**: Any external consumers of the API (e.g. FHIRPath Lab frontend) must update their request URLs. This is a breaking change.
