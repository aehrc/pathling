# 03 — Engine-built System.Quantity takes one fixed shape, not the legacy one

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-059. This is the Quantity-shape slice of #2804, which keeps the System.Coding half.

**What to build:** A `System.Quantity` the engine builds itself, such as a quantity literal or the result of an operation, leaves the engine in one fixed, complete shape whatever produced it. It no longer carries the legacy layout's shape: no `DECIMAL` value with a `value_scale` companion, no `_value_canonicalized` and `_code_canonicalized` companions, and no `_fid`. The renderer that turns engine values into FHIR JSON can then emit a System.Quantity without going through `QuantityNormalisation`.

**Blocked by:** None — can start immediately

**Status:** done

- [x] Tests pin the struct type of an engine-built System.Quantity: field names, types and order. They cover a literal, an arithmetic result and a union of a literal with a stored quantity.
- [x] No legacy companion field appears in that type.
- [x] Rendering a System.Quantity no longer calls `QuantityNormalisation`. That call is removed, and rendering tests pass.
- [x] Quantity comparison and arithmetic results are unchanged. The existing `QuantityCollection` tests pass without edits to their expectations. _(The `QuantityCollection` tests pass unedited. One result changed and was accepted as a correction: `5.toQuantity() = 500 '%'` went from empty to `true`, as the FHIRPath specification requires. See the comments.)_
- [x] The fixed shape is documented where FR-059 asks for it.

## Comments

### Implementation (branch `issue/2829-system-quantity`)

An engine-built System.Quantity now has one fixed shape, whatever produced it:

```
struct<id: string, value: string, comparator: string, unit: string, system: string, code: string>
```

These are the elements of a FHIR Quantity other than `extension`, in definition order, all present.
The value is the text of the decimal, as the new layout stores one, so it keeps its own scale without
a `value_scale` companion. `id` and `comparator` are null for a literal, a conversion or a promoted
number. A stored quantity taken as a System one (decision 82), for example in `(stored | 1 'g')`,
keeps its stored id and comparator, as `decodeStored` did before. There is no canonical form and no
`_fid`.

- `QuantityEncoding.dataType()` is that type. `decodeStored` is now `fromStored`, which projects a
  stored quantity into it rather than decoding it.
- The decimal value and the canonical form are computed where a quantity is compared or converted
  (`QuantityEncoding.decodeValue`, `canonicalValue` and `canonicalCode`, used by `QuantityValue`), by
  the same UDFs a stored quantity already used. So stored and System quantities share one path.
- A literal's canonical form used to come from `FhirPathQuantity.asCanonical()`, which maps a definite
  calendar duration (`second`, `millisecond`) to its UCUM equivalent. The computed path does the same
  mapping where the system is the FHIRPath calendar-duration URI, using the previously unused
  `CALENDAR_DURATION_TO_UCUM` map.
- The UDFs (`StringToQuantity`, `ConvertQuantityToUnit`, `QuantityToLiteral`) read and write Rows in
  the fixed shape. `decode(Row)` rounds the value text to `DECIMAL(32,6)` semantics, so `toString()`
  and conversion results are unchanged. `QuantityEncodingTest` pins that rounding.
- `SingleInstanceEvaluator` no longer calls `NormaliseQuantity`. A System quantity is rendered as it
  is, by its definition. `QuantityNormalisation`, its `ResolveOrNull` arm and `quantityColumnOrNull`
  are untouched; ticket 05 deletes them.
- Tests: `SystemQuantityShapeTest` pins the type against a struct written out in the test, and checks
  for legacy companions. The producers it covers are a literal, a calendar-duration literal, integer
  and decimal arithmetic results promoted with `toQuantity()`, a string converted with
  `toQuantity()`, unit conversions of a literal and of a stored quantity, and unions of a literal with
  a stored quantity in both orders. It also pins field values and comparisons, including calendar
  durations. `EvaluateFhirPathTest` gains rendering cases for `1.50 'mg'` and `(1 + 2).toQuantity()`.
- Docs: the shape is in `contracts/engine-semantics.md` (Result types and Quantities). The
  user-visible change is in `contracts/library-api.md` under Behaviour changes.
- `QuantityCollectionTest` passes with no edits to it.
- Test runs on the final commit:
  - the full `fhirpath` suite: 7630 run, 0 failed, 922 skipped;
  - the same suite with `-Dpathling.testLayout=previous`: 7630 run, 0 failed, 924 skipped;
  - the SQL-on-FHIR compliance tests, with the submodule initialised: 125 run, 0 failed;
  - the `library-api` suite: 318 run, 0 failed.

### Restated assertions, for review

- **`QuantityTypesTest.projectedColumnHasTheDecodedStructure`** is renamed to
  `projectedColumnIsASystemQuantity`. It asserted that the projected column carries `value_scale` 0
  and the canonical fields. The column no longer carries them, so it now computes the canonical
  value and code from the column with `QuantityEncoding.canonicalValue` and `canonicalCode`. The
  expected canonical values are unchanged.
- **The YAML harness** builds an expected quantity in the fixed shape, with the value as plain text
  (`YamlSupport.writeQuantity`). `DefaultYamlTestExecutor.normalizeQuantityRow` compares the value
  rounded to six fractional digits, as the engine compares decimals. It no longer compares the text,
  which would distinguish `1.5` from `1.50`. Before, both sides were `DECIMAL(32,6)`.

### Behaviour that differs, for review

- **A number promoted to a quantity now has a canonical form.** `encodeNumeric` used to write a null
  canonical form. In the fixed shape, a promoted number cannot be told apart from the literal
  `5 '1'`, so it is canonicalised as that literal always was. So `5.toQuantity() = 500 '%'` is now
  `true`, where it was empty. This was confirmed by running `SystemQuantityShapeTest` against the
  base branch's code. `5 '1' = 500 '%'` was already `true`. The same applies to any comparison of a
  promoted number with a unit whose canonical code is `1`, such as `[ppm]` or `10*3`.
  **Accepted as a correction.** The specification says `toQuantity()` on an Integer or Decimal
  gives a quantity with "the default unit (`'1'`)", and quantities of the same dimension compare
  whatever their units. The reference implementation, fhirpath.js 5.1.0, builds the same quantity
  for `5.toQuantity()` as for `5 '1'`. The old empty result was a defect.
  `SystemQuantityShapeTest.promotedNumberComparesAsTheDimensionlessLiteral` pins it.
- **The rendered value of a System quantity with more than six fractional digits** is now its full
  text. Before, `NormaliseQuantity` rendered it at six digits. This covers a literal such as
  `0.0000002 'kg'` and a conversion result such as `(1 'g').toQuantity('[lb_av]')`. Comparison,
  `toString()` and `.value` still decode to six digits. JSON keeps the full value, which the
  canonical form is computed from, so the JSON can show more digits than `toString()`.
  **Accepted.** The rendered value is the more faithful one. The limit on `toString()` and `.value`
  is the engine's decimal type, which this ticket does not change.
- **A stored quantity whose system is the FHIRPath calendar-duration URI** is now canonicalised
  through the definite-duration map, not from its raw code (decision 77 says "whatever the system").
  This is an edge case that real data is unlikely to contain.
- **Comparing against a literal now calls the canonicalisation UDFs on the literal side too**, once
  per row. Before, the literal carried its canonical form as a constant. This has not been measured.
  **Deferred to ticket 12**, which restores it by giving the System.Quantity shape the canonical
  annotation fields (item 5 there).
- **A promoted number's value is its shortest text.** `encodeNumeric` gave every number promoted to
  a quantity six fractional digits, so `5.toQuantity()` rendered as `5.000000`. Its scale is not
  known, so it now has no trailing fractional zeros: `5`, `(1.5 * 2)` as `3`, and `1.25`. Found in
  review. `.value`, `toString()` and comparison are unchanged, because they decode the text.
  `SystemQuantityShapeTest` pins integer, decimal, zero and Boolean sources.
- **`toQuantity(unit)` no longer keeps a stored id and comparator when the unit already matches.**
  It used to return the stored quantity unchanged on that path, but build a new one without them
  when it converted, so `toQuantity('mg')` lost a `<` that `toQuantity('g')` kept. Both paths now
  return a quantity the engine built, as a conversion is described above. This predates the
  ticket and was found in review. `StoredQuantityTest.conversionDropsIdAndComparator` pins it.
- **Incidental fix:** `(1 'g').toQuantity('mg')` over a dataset raised
  `INTERNAL_ERROR: Cannot evaluate expression: value` before this change, in `SystemQuantityShapeTest`'s
  first, red run. It now returns `1000 'mg'`.
- **The `pathling.testLayout=previous` dimension** builds its synthetic quantities with
  `QuantityEncoding.dataType()`, so on that dimension they are now in the fixed shape rather than a
  previous-layout one. They therefore no longer exercise the legacy normalisation. The `fhirpath`
  suite passes under that dimension too. Ticket 05 removes the dimension.

