## Why

When the fhirpath-lab-api receives an empty FHIRPath expression, it passes it
through to the Pathling engine which throws a Java-side error, resulting in a 500
response with an OperationOutcome. An empty expression should instead return an
empty collection, which is the natural FHIRPath semantics for "no evaluation".

## What changes

- Add early detection of empty (blank or whitespace-only) expressions in the
  fhirpath-lab-api request handler.
- Return a valid Parameters response with no result parts, matching the existing
  empty-result format.

## Capabilities

### New capabilities

_(none)_

### Modified capabilities

- `fhirpath-evaluation-api`: Add requirement that an empty expression returns an
  empty collection rather than an error.

## Impact

- `fhirpath-lab-api/src/fhirpath_lab_api/app.py` — request handler logic.
- No upstream library changes required.
- No breaking changes to the API contract; callers currently receiving an error
  for empty expressions will instead receive a successful empty response.
