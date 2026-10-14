# 03 — Engine-built System.Quantity takes one fixed shape, not the legacy one

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-059. This is the Quantity-shape slice of #2804, which keeps the System.Coding half.

**What to build:** A `System.Quantity` the engine builds itself, such as a quantity literal or the result of an operation, leaves the engine in one fixed, complete shape whatever produced it. It no longer carries the legacy layout's shape: no `DECIMAL` value with a `value_scale` companion, no `_value_canonicalized` and `_code_canonicalized` companions, and no `_fid`. The renderer that turns engine values into FHIR JSON can then emit a System.Quantity without going through `QuantityNormalisation`.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Tests pin the struct type of an engine-built System.Quantity: field names, types and order. They cover a literal, an arithmetic result and a union of a literal with a stored quantity.
- [ ] No legacy companion field appears in that type.
- [ ] Rendering a System.Quantity no longer calls `QuantityNormalisation`. That call is removed, and rendering tests pass.
- [ ] Quantity comparison and arithmetic results are unchanged. The existing `QuantityCollection` tests pass without edits to their expectations.
- [ ] The fixed shape is documented where FR-059 asks for it.
