## Why

String search parameters whose FHIRPath expression resolves to a complex type
(HumanName, Address) cause a Spark `DATATYPE_MISMATCH` error because
`StringMatcher` applies `lower()` directly to the struct column instead of its
string sub-fields. For example, `Patient?name=paul` fails because `Patient.name`
evaluates to an `ARRAY<STRUCT<...>>` of HumanName, but the matcher expects a
plain string.

## What changes

- `SearchColumnBuilder.buildExpressionFilter()` will detect when a search
  parameter expression evaluates to a complex type (HumanName, Address) and
  expand it into multiple sub-field expressions that each resolve to a string
  column.
- For HumanName: expand to `family`, `given`, `text`, `prefix`, `suffix`.
- For Address: expand to `text`, `line`, `city`, `district`, `state`,
  `postalCode`, `country`.
- Each sub-field expression is evaluated independently and the results are OR'd
  together, so the search matches if any sub-field starts with the search value.
- The `StringMatcher` and `ExactStringMatcher` remain unchanged; they continue
  to operate on simple string columns.

## Capabilities

### New capabilities

(none)

### Modified capabilities

(none - this is a bug fix against existing requirements in `server-search-params`
spec, which already specifies that `name=eve` should match "any name component")

## Impact

- `SearchColumnBuilder` in the `fhirpath` module (primary change).
- No API changes, no breaking changes.
- Affects all string-type search parameters that resolve to HumanName or Address
  types across all resource types.
