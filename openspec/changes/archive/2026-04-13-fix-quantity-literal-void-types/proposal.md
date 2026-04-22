## Why

Quantity literals produce Spark structs with `VOID`-typed null fields (e.g., `id`,
`comparator`, `_fid`, and for indefinite calendar durations `_value_canonicalized`
and `_code_canonicalized`). `VOID` is Spark's placeholder for untyped nulls —
not a concrete data type. Any code path that requires a concrete schema (such as
`to_variant_object()` used by `repeat()` / `repeatAll()`) rejects these structs
with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`. This blocks `repeat($this)`
over any Quantity literal collection and surfaces via `combine()` + `repeat()`
(issue #2588).

## What Changes

- Cast the Quantity literal struct to `QuantityEncoding.dataType()` after
  construction so every field carries its declared type instead of `VOID`.
- Cast the `FlexiDecimalSupport.toLiteral` null branch to
  `FlexiDecimal.DATA_TYPE` so the nested canonical-value sub-struct is also
  properly typed.
- Remove the two `config.yaml` exclusions that were filed under #2588 and
  verify the expressions pass.
- Also remove or downgrade the related "Indefinite calendar duration union
  deduplication" `wontfix` exclusion if the fix resolves it.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

_None — this is a bug fix in the encoding layer; no spec-level behavior changes._

## Impact

- `QuantityEncoding.encodeLiteral` (fhirpath module) — one-line `.cast(dataType())` addition.
- `FlexiDecimalSupport.toLiteral` (encoders module, Scala) — change `lit(null)` to `lit(null).cast(DATA_TYPE)`.
- `config.yaml` test exclusions — remove #2588 entries and evaluate the related
  indefinite calendar duration exclusion.
- Risk: low — the cast merely concretises types that were already correct
  structurally; no runtime values change.
