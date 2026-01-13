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

For `eq` and `ne` prefixes with canonical values, Pathling uses **exact equality comparison** rather than range-based semantics. This is because:
1. Canonicalization already normalizes values to a consistent form
2. Range semantics based on significant figures are less meaningful after unit conversion
3. Exact comparison provides more predictable behavior

**Rationale:** If a user searches for `1000|http://unitsofmeasure.org|mg`, this canonicalizes to approximately `0.001 g`. An exact match against the resource's canonical value is more intuitive than applying range semantics to the original "1000" value.

#### Fallback Behavior

If canonicalization fails (e.g., unknown unit, invalid code), the matcher:
1. Does **not** throw an error
2. Falls back to standard value comparison with exact system/code matching
3. This ensures searches remain functional even for non-standard units

### Implementation

- `QuantityMatcher.java` - Contains `tryMatchWithUcumNormalization()` method
- Uses `Ucum.getCanonical()` for value/unit canonicalization
- Compares against `_value_canonicalized` and `_code_canonicalized` fields in Quantity struct
