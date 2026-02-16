## Why

The Pathling server currently only supports search via FHIRPath expressions
through a `_query=fhirPath` named query. The library API already provides a
`searchToColumn` method backed by `SearchColumnBuilder` that supports standard
FHIR search parameters (token, string, date, number, quantity, reference, URI).
Exposing these as native search parameters on the server would make it
FHIR-conformant for search, discoverable via the CapabilityStatement, and
interoperable with standard FHIR clients without requiring knowledge of
FHIRPath.

## What changes

- Standard FHIR search parameters become available directly on resource type
  search endpoints (e.g., `GET /Patient?gender=male&birthdate=ge1990-01-01`).
- Standard search parameters can be combined with FHIRPath `filter` parameters
  within the same request (AND logic between all criteria).
- The `_query=fhirPath` named query continues to work unchanged for
  backwards compatibility.
- The CapabilityStatement declares all supported search parameters per resource
  type, with correct types and documentation.
- Search via POST `_search` also supports standard parameters.

## Capabilities

### New capabilities

- `server-search-params`: Standard FHIR search parameter support on the server,
  including parameter parsing, delegation to `SearchColumnBuilder`, combination
  with FHIRPath filters, and CapabilityStatement declaration.

### Modified capabilities

(none)

## Impact

- **Server module**: `SearchProvider`, `SearchExecutor`, and
  `ConformanceProvider` require changes.
- **Dependencies**: Server already depends on `library-runtime` which bundles
  `SearchColumnBuilder` and the search parameter registry.
- **API surface**: Additive only; existing `_query=fhirPath` behaviour is
  preserved.
- **Documentation**: The server search documentation needs updating to cover
  standard search parameters and combined usage.
