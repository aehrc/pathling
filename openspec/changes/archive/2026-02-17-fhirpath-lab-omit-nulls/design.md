## Context

The FHIRPath Lab API serialises complex type results (e.g. Quantity, HumanName)
as JSON strings via the `json-value` extension. The underlying Pathling
evaluation returns dicts that include keys with `None` values for unset fields.
Python's `json.dumps` faithfully serialises these as `null`, cluttering the
output.

## Goals / Non-Goals

**Goals:**

- Remove null-valued keys from complex type result dicts before JSON
  serialisation.
- Apply recursively to handle nested structs.

**Non-Goals:**

- Changing the Pathling library API itself; this is a presentation concern in
  the lab API layer only.
- Filtering empty lists or zero values; only `None` values are stripped.

## Decisions

**Strip nulls at serialisation time in `_build_result_part`**: Add a recursive
helper that removes keys with `None` values from a dict. Apply it to the value
dict before calling `json.dumps`. This keeps the change localised to the
response formatting code.

Alternative considered: Stripping nulls at the Pathling library API level. This
would affect all consumers and conflates a presentation preference with core
API semantics.

## Risks / Trade-offs

- **Semantic loss**: A field explicitly set to `null` in FHIR is different from
  an absent field in some contexts. In practice, the values coming from Spark
  column extraction are simply unset (not explicitly null), so omission is
  correct. No mitigation needed.
