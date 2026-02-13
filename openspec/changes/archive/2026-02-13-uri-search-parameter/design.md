## Context

The search parameter framework in the `fhirpath` module implements FHIR search parameter types as `ElementMatcher` implementations, wrapped by `SearchFilter` for array/scalar handling and negation. Six of the seven in-scope types are already implemented (token, string, date, number, quantity, reference). The `URI` enum constant is stubbed with an empty allowed-types set and the default `createFilter()` that throws `UnsupportedOperationException`.

URI search parameters use exact string equality by default. The FHIR specification also defines `:below` (prefix matching) and `:above` (inverse prefix matching) modifiers for URI parameters. The `ExactStringMatcher` already implements exact equality for the `:exact` modifier on string search, but a dedicated class is warranted to accommodate the additional matching modes.

## Goals / Non-goals

**Goals:**

- Implement URI search parameter matching with exact string equality.
- Support the `:below` modifier for URI prefix matching.
- Support the `:above` modifier for inverse URI prefix matching.
- Support the `:not` modifier for negated matching (Pathling extension, not defined for URI in R4 but consistent with other Pathling search parameter types).
- Declare all URI family FHIR types as allowed.
- Follow the established pattern used by other search parameter types.

**Non-goals:**

- No changes to the server module — the search framework already dispatches by type.

## Decisions

**Reuse vs. new class**: Create a dedicated `UriMatcher` class rather than reusing `ExactStringMatcher`. The class needs to support three matching modes (exact, below, above), so it diverges from `ExactStringMatcher` in both interface and behaviour. A factory method or enum selects the mode at construction time.

**Allowed FHIR types**: The allowed set includes all URI family types: `URI`, `URL`, `CANONICAL`, `OID`, and `UUID`. The FHIRPath evaluator preserves the specific subtype from HAPI's `@DatatypeDef` annotation (e.g., `CapabilityStatement.url` resolves to `FHIRDefinedType.URL`, not `URI`). All five subtypes map to `FhirPathType.STRING` and encode as Spark `StringType`, so the matching logic is identical for all of them.

**`:below` semantics**: The search value is treated as a URI prefix. The matcher returns true when the element value starts with the search value. Per the FHIR specification: `url:below=http://example.org/fhir/` matches any URI that begins with that string.

**`:above` semantics**: The inverse of `:below`. The matcher returns true when the search value starts with the element value. Per the FHIR specification: `url:above=http://example.org/fhir/ValueSet/123` matches resources whose URL is a prefix of the search value (i.e., the resource's URI is "above" the search value in the URI hierarchy).

**Modifier support**: `:not`, `:below`, and `:above` are supported. `:not` is a Pathling extension consistent with other search parameter types (e.g., `REFERENCE`). All other modifiers throw `InvalidModifierException`.

## Risks / Trade-offs

[Minimal risk] The implementation is straightforward string operations (equality, starts-with). The main risk was that URI-type search parameters could resolve to FHIR subtypes not in the allowed set — this is mitigated by including all five URI family types and integration testing with parameters that resolve to different subtypes.
