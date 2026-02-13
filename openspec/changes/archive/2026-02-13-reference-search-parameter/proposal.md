## Why

The FHIR search implementation (issue #1986) supports Token, Date, Quantity, String, and Number parameter types but not Reference. Reference search is essential for filtering resources by their relationships to other resources (e.g., finding Observations for a specific Patient via `subject=Patient/123`). Adding this enables cohort queries that combine clinical criteria with resource relationships.

## What changes

- Implement the `REFERENCE` search parameter type in the FHIRPath search filter framework.
- Add a `ReferenceMatcher` that matches the `reference` field within the FHIR Reference struct against search values in all FHIR-specified formats: `[id]`, `[type]/[id]`, and absolute URLs.
- Register `FHIRDefinedType.REFERENCE` as the allowed FHIR type for the `REFERENCE` search parameter type.
- Support the `:not` modifier for negated reference matching and the `:[type]` modifier for constraining the target resource type of a polymorphic reference parameter.
- Validate search values, rejecting malformed inputs such as bare type names or trailing slashes.
- Reference search parameters from the FHIR spec (e.g., `Observation.subject`, `Encounter.patient`) will be loaded by the existing registry and become usable in search queries.

## Capabilities

### New capabilities

- `reference-search`: Matching logic for the FHIR reference search parameter type, supporting relative references (`Patient/123`), absolute URLs (`http://example.org/fhir/Patient/123`), and bare IDs (`123`).
- `reference-search-not-modifier`: Negated reference matching via the `:not` modifier (e.g., `subject:not=Patient/123`).
- `reference-search-type-modifier`: Type-constrained reference matching via the `:[type]` modifier (e.g., `subject:Patient=123`), which prepends the type to bare ID values before matching.

### Modified capabilities

## Impact

- `fhirpath` module: `SearchParameterType.REFERENCE`, new `ReferenceMatcher` class, updated `FhirFieldNames` with reference field constant, tests.
- Consolidate the `REFERENCE_ELEMENT_NAME` constant from `ReferenceCollection` into `FhirFieldNames.REFERENCE` for consistency.
- No API changes — reference parameters are already defined in the bundled search parameter registry and will start working once the type is implemented.
- No breaking changes.
