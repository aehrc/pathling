## Why

The URI search parameter type is one of the seven search parameter types defined in the FHIR Search API (issue #1986). It is currently stubbed out in `SearchParameterType.URI` with an empty set of allowed types and no implementation. There are 45 URI-type search parameters defined in the FHIR R4 specification (e.g., `url`, `source`, `policy`), making this a common parameter type that users will encounter.

## What changes

- New `UriMatcher` class implementing `ElementMatcher` with exact, `:below` (prefix), and `:above` (inverse prefix) matching modes.
- Update `SearchParameterType.URI` to declare all URI family types (`URI`, `URL`, `CANONICAL`, `OID`, `UUID`) as allowed and override `createFilter()`.
- Support the `:not`, `:below`, and `:above` modifiers.
- Reject all other modifiers with `InvalidModifierException`.
- Unit and integration tests for URI matching behaviour, covering multiple URI subtypes.

## Capabilities

### New capabilities

- `uri-search`: URI search parameter type matching URIs by exact string equality, with `:not`, `:below`, and `:above` modifier support.

### Modified capabilities

(none)

## Impact

- `fhirpath` module: new `UriMatcher` class, updated `SearchParameterType` enum.
- No API changes — the search framework already dispatches by type; this fills in the missing implementation.
- No dependency changes.
