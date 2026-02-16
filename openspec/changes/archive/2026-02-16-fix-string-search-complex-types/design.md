## Context

The `SearchColumnBuilder.buildExpressionFilter()` method evaluates a FHIRPath
expression (e.g. `Patient.name`) and passes the resulting column to a
`SearchFilter` which delegates to `StringMatcher`. When the expression resolves
to a complex type like HumanName, the column is a struct, and `lower()` fails
with a Spark `DATATYPE_MISMATCH` error.

The existing flow:

```
FHIRPath("Patient.name") → Collection(HUMANNAME, ARRAY<STRUCT<...>>)
  → StringMatcher.match(structColumn, "paul")
    → lower(structColumn)  ← FAILS
```

## Goals / Non-goals

**Goals:**

- String and exact-string search parameters work correctly when the FHIRPath
  expression resolves to HumanName or Address types.
- The fix follows FHIR spec requirements for which sub-fields to search within
  each complex type.

**Non-goals:**

- Supporting other complex types beyond HumanName and Address (these are the
  only complex types in the STRING search parameter type's allowed list).
- Changing the `StringMatcher` or `ExactStringMatcher` interfaces.

## Decisions

### Expand complex type expressions into sub-field expressions

When `buildExpressionFilter()` detects that the evaluated FHIRPath expression
resolves to a complex type (HumanName or Address), it will expand the single
expression into multiple sub-field FHIRPath expressions, evaluate each
independently, and OR the filter results together.

For example, `Patient.name` with search value `paul` becomes:

```
Patient.name.family  → StringMatcher → filter1
Patient.name.given   → StringMatcher → filter2
Patient.name.text    → StringMatcher → filter3
Patient.name.prefix  → StringMatcher → filter4
Patient.name.suffix  → StringMatcher → filter5

result = filter1 OR filter2 OR filter3 OR filter4 OR filter5
```

Each sub-field expression resolves to either a `STRING` or `ARRAY<STRING>`
column, which `StringMatcher` already handles via `SearchFilter.buildFilter()`'s
vectorize logic.

**Alternatives considered:**

- _Make StringMatcher type-aware_: Would require matchers to understand FHIR
  type structure, mixing matching and type decomposition concerns.
- _Transform the ColumnRepresentation before matching_: Would require runtime
  type introspection on Spark columns, which is fragile.

### Define sub-field mappings as a static lookup

The mapping from complex type to string sub-fields will be a simple static map
within `SearchColumnBuilder`:

- `HUMANNAME` → `family`, `given`, `text`, `prefix`, `suffix`
- `ADDRESS` → `text`, `line`, `city`, `district`, `state`, `postalCode`,
  `country`

These match the FHIR specification's string search definitions for each type.

## Risks / Trade-offs

- **Multiple FHIRPath evaluations per complex-type expression** → Each sub-field
  requires a separate parse and evaluate call. This is acceptable because (a)
  the number of sub-fields is small and bounded, (b) evaluation only builds
  column expressions (no data is read), and (c) this only applies to the subset
  of search parameters with complex types.
- **Maintenance burden for sub-field lists** → If FHIR adds new string fields to
  HumanName or Address in future versions, the mapping needs updating. This is
  low risk since these types are stable and well-established.
