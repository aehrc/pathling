## Context

The FHIR search implementation in the `fhirpath` module uses a `SearchParameterType` enum with a `MatcherFactory` pattern. Each type declares its allowed FHIR types and creates an `ElementMatcher` via `createFilter()`. The `SearchFilter` class handles array/scalar vectorisation and OR/negation logic. Five types are implemented (Token, String, Date, Number, Quantity); Reference is declared but has an empty allowed-types set and falls through to the default `UnsupportedOperationException`.

FHIR Reference elements are encoded as structs with a `reference` string field (e.g., `"Patient/123"` or `"http://example.org/fhir/Patient/123"`). The existing `ReferenceCollection` already extracts the `reference` field for joins and type detection.

## Goals / Non-goals

**Goals:**

- Support reference search parameter matching against the `reference` field of Reference structs.
- Handle all three FHIR-specified value formats: bare ID (`123`), type-qualified (`Patient/123`), and absolute URL.
- Support the `:not` modifier for negated matching (leveraging existing `SearchFilter` negation).
- Support the `:[type]` modifier for constraining the target resource type of a polymorphic reference (e.g., `subject:Patient=123`).
- Validate search values and reject malformed inputs.
- Follow the existing matcher pattern so the implementation slots in with zero changes to `SearchFilter` or `SearchColumnBuilder`.

**Non-goals:**

- Chained reference parameters (e.g., `subject.name=Smith`).
- The `:identifier` modifier (searching by logical reference identifier).
- The `:missing` modifier.

## Decisions

### Match against the `reference` string field

The `ReferenceMatcher` will extract the `reference` field from the Reference struct and match it against the search value. This mirrors how `ReferenceCollection.getKeyCollection()` already works.

**Alternative considered:** Matching against a parsed/normalised form of the reference. Rejected because: (a) the existing data stores references as-is, and (b) matching the raw string with suffix-based logic is simpler and handles all standard formats.

### Suffix-based matching strategy

A search value of `Patient/123` should match both `Patient/123` (relative) and `http://example.org/fhir/Patient/123` (absolute). The matcher will use `endsWith` semantics: the reference string must end with the search value, and the character immediately before the match (if any) must be `/` to avoid partial matches (e.g., `APatient/123`). When the reference string is exactly equal to the search value (i.e., a relative reference matching a type-qualified search value), this is also a match.

For bare ID values (no `/`), the matcher requires `endsWith("/" + id)` to ensure the ID matches the final segment of the reference.

**Alternative considered:** Regex matching. Rejected for simplicity — `endsWith` combined with a boundary check is sufficient and avoids regex compilation overhead.

### Absolute URI detection

A search value is treated as an absolute URI when it starts with `http://`, `https://`, `urn:uuid:`, or `urn:oid:`. Absolute URI search values are matched using exact string equality against the `reference` field.

### Search value validation

The matcher will validate the search value before matching. Values that are empty, consist of only a resource type with no ID (e.g., `Patient`), or end with a trailing slash (e.g., `Patient/`) will produce no matches. This is enforced through the matching logic itself: a bare value with no `/` is treated as an ID and must match the final path segment, so a bare type name like `Patient` would only match a reference ending in `/Patient`, which is not a valid FHIR reference.

### `:not` modifier support

The `:not` modifier uses the existing `SearchFilter` negation mechanism. The `createFilter()` method wraps the `ReferenceMatcher` in a `SearchFilter` with `negated=true`, identical to how TOKEN handles `:not`. This is a one-line addition with no new code paths.

### `:[type]` modifier support

The `:[type]` modifier constrains a polymorphic reference parameter to a specific resource type. When the modifier is a valid FHIR resource type name (e.g., `Patient`), the `createFilter()` method prepends the type to each search value before matching. For example, `subject:Patient=123` is equivalent to `subject=Patient/123`. This is implemented by wrapping the `ReferenceMatcher` in a `SearchFilter` that transforms search values via a type-qualifying prefix.

The modifier value is validated against the set of known FHIR resource type names. An unrecognised modifier (neither `not` nor a valid resource type) throws `InvalidModifierException`.

### Consolidate reference field constant

The `ReferenceCollection` class defines a private `REFERENCE_ELEMENT_NAME` constant. This will be replaced with a shared `FhirFieldNames.REFERENCE` constant for consistency with how other matchers access struct field names.

## Risks / Trade-offs

- **Bare ID ambiguity**: A search value like `123` will match any reference ending in `/123` regardless of resource type. This matches FHIR spec behaviour (servers should know the type from the parameter definition) but could produce false positives if references to different resource types share the same ID. Users can avoid this by using type-qualified searches (`Patient/123`) or the `:[type]` modifier (`subject:Patient=123`).
- **Absolute URL matching**: The `endsWith` approach means `http://server-a.com/Patient/123` will match a search for `http://server-b.com/Patient/123` only if the full URL is provided. Relative references (`Patient/123`) will correctly match absolute URLs. This aligns with FHIR spec behaviour.
- **`urn:` references require exact match**: References using `urn:uuid:` or `urn:oid:` schemes can only be matched by providing the exact URI as the search value. Suffix-based matching (bare ID or type-qualified) is not meaningful for these schemes. This is acceptable because `urn:` references are primarily transient (used within Bundle transactions) and the exact URI is the only sensible way to search for them.
