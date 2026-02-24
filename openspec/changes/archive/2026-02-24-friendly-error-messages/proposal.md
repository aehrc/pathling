## Why

When FHIRPath evaluation fails in the fhirpath-lab-api, the error message
returned to the user includes the full Java stack trace from the Py4J bridge.
This makes error messages unreadable for end users who just need to know what
went wrong with their expression.

## What Changes

- Parse Py4J Java exceptions to extract a human-friendly error message
  (e.g. "UnsupportedFhirPathFeatureError: Unsupported function: trace").
- Place the full exception string (including stack trace) in the
  OperationOutcome `diagnostics` element for debugging purposes.
- Non-Py4J exceptions pass through unaltered.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `fhirpath-lab-server`: Error handling requirement updated to distinguish
  between human-readable message and full diagnostic details.

## Impact

- `fhirpath-lab-api/src/fhirpath_lab_api/app.py`: Error handling in the
  evaluation endpoint.
- No API contract changes — the response is still an OperationOutcome, just
  with better content.
