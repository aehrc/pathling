## Context

The `SingleInstanceEvaluator.sanitiseRow()` method currently strips synthetic
fields (names starting with `_` or ending with `_scale`) from Spark Rows before
JSON serialisation. However, it does not strip fields with null values, so
Spark's `Row.json()` includes them in the output. The Python `_strip_nulls`
function in `parameters.py` was intended to handle this, but is unreachable
because complex types arrive as pre-serialised JSON strings from Java.

## Goals / Non-goals

**Goals:**

- Produce clean JSON for complex type results by stripping null-valued fields at
  the Java layer, before `Row.json()` serialises them.
- Remove the dead Python `_strip_nulls` code path.

**Non-goals:**

- Changing the serialisation format or encoding of complex types.
- Modifying how primitive types are handled.

## Decisions

### Strip nulls in `sanitiseRow()`

Add a null-value check to the existing field iteration loop in `sanitiseRow()`.
A field is excluded if `SyntheticFieldUtils.isSyntheticField(name)` returns true
**or** if the value is null. This keeps the logic in a single pass and avoids a
second traversal.

**Alternative considered:** Parse and re-serialise JSON in the Python layer.
Rejected because it adds unnecessary overhead and keeps the fix far from the
source of the problem.

### Remove `_strip_nulls` from Python

With null stripping handled in Java, the `_strip_nulls` function and the
`isinstance(value, str)` branching in `_build_result_part` become unnecessary.
The complex type branch can unconditionally treat values as strings (which is
what the Java side always produces).

## Risks / Trade-offs

- **Risk:** Stripping nulls changes the JSON output for all consumers of
  `SingleInstanceEvaluator`, not just the FHIRPath Lab API.
  **Mitigation:** This is desirable behaviour; null fields carry no information
  and are not expected by FHIR consumers.
