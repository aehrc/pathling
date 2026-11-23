# FHIR Specification Clarifications

This document captures clarifications and interpretations of ambiguous or unclear aspects of the FHIR specification as implemented in Pathling.

## Quantity Search Parameter Format

**FHIR Spec Reference:** [Search - Quantity](https://hl7.org/fhir/search.html#quantity)

### Issue

The FHIR specification defines the quantity search format as `[prefix][number]|[system]|[code]` but does not clearly specify:
1. Which combinations of empty/present system and code are valid
2. The semantics of an empty system component (e.g., `5.4||kg`)

### Clarification

Based on analysis of the token search parameter specification and practical considerations, Pathling implements the following interpretation:

#### Valid Formats

| Format | Example | System | Code | Description |
|--------|---------|--------|------|-------------|
| `[number]` | `5.4` | any | any | Value-only search |
| `[number]\|[system]\|[code]` | `5.4\|http://unitsofmeasure.org\|kg` | exact | exact | Full match with system and code |
| `[number]\|\|[code]` | `5.4\|\|kg` | any | exact | Match code in any system |

#### Invalid Formats

| Format | Example | Reason |
|--------|---------|--------|
| `[number]\|[system]\|` | `5.4\|http://unitsofmeasure.org\|` | Empty code with non-empty system is not supported |

#### System Semantics

An empty or omitted system component is interpreted as "match any system" (no constraint). This differs from token search where `|[code]` specifically matches resources where the system is not present (null).

**Rationale:** The quantity search format `[number]|[system]|[code]` is structurally different from token search `[system]|[code]`. Requiring users to match null-system quantities would be an unusual use case, and the simpler "any system" interpretation is more practical.

### Implementation

- `QuantitySearchValue.java` - Parses and validates the format
- `QuantityMatcher.java` - Implements the matching logic

---

## Quantity Search UCUM Normalization

**FHIR Spec Reference:** [Search - Quantity](https://hl7.org/fhir/search.html#quantity)

### Issue

The FHIR specification mentions that quantity searches "should" support UCUM unit conversion but does not specify the exact implementation details:
1. When should normalization be applied?
2. How should failures in normalization be handled?
3. Should range semantics (eq/ne) apply to canonicalized values?

### Clarification

Pathling implements UCUM normalization with the following behavior:

#### When UCUM Normalization Applies

UCUM normalization is applied when **all** of the following conditions are met:
1. The search specifies a system component
2. The system is the UCUM system (`http://unitsofmeasure.org`)
3. The search specifies a code component
4. The UCUM library successfully canonicalizes the search value and code

If any of these conditions is not met, the matcher falls back to standard value comparison.

#### Normalization Behavior

When UCUM normalization applies:
- The search value and code are canonicalized using the UCUM library
- Comparison is performed against pre-computed canonical values in the resource's Quantity struct
- This enables matching across equivalent unit representations (e.g., `1000|http://unitsofmeasure.org|mg` matches a resource with value `1 g`)

#### Range Semantics for Canonical Values

For `eq` and `ne` prefixes with canonical values, Pathling matches the **implicit range** of the search value, as it does for a number search and for a quantity search without a UCUM system. The specification's own example gives `5.4|http://unitsofmeasure.org|mg` the range 5.4 ± 0.05 mg.

1. The search value is rounded to the scale of the engine's System.Decimal, `DECIMAL(38,6)`, as the canonical form of a stored value is computed from its rounded value.
2. The implicit range is taken from the significant figures of the search value, so `5.40` has the narrower range [5.395, 5.405).
3. Both boundaries of the range are canonicalised, and the stored value's canonical value is compared with them. So `5.4|http://unitsofmeasure.org|mg`, the range [5.35, 5.45) mg, matches a stored `5.41 mg` and a stored `0.00541 g`, and `ne` matches neither.
4. A unit whose canonical value falls as its value rises, such as `[pH]`, reverses the order of the canonical boundaries, and the range still includes the lower boundary of the search value and excludes the upper.

The comparison prefixes `gt`, `ge`, `lt` and `le` compare the stored value's canonical value with the canonical form of the search value itself.

#### Fallback Behavior

If canonicalization fails (e.g., unknown unit, invalid code), the canonical form of the search value or of a boundary of its range has more than 38 integer digits, or the canonical search value does not lie strictly between the canonical boundaries (as for `[m/s2/Hz^(1/2)]`, which ucumate squares, so that the boundaries of `0` both canonicalise to 0.25), the matcher:
1. Does **not** throw an error
2. Falls back to standard value comparison with exact system/code matching, which also matches the implicit range under `eq` and `ne`
3. This ensures searches remain functional even for non-standard units

The same fallback applies to each stored UCUM quantity that has no canonical form. Because both paths match the implicit range, one search no longer matches some stored quantities by a range and others exactly.

### Implementation

- `QuantityMatcher.java` - Contains `tryMatchWithUcumNormalization()` method
- Uses the ucumate library, through `UcumUnits.canonicalize()`, for value/unit canonicalization
- Compares against the `__canonical_code` and `__canonical_value_exact` annotations of the stored Quantity, or computes them where the schema does not carry them
