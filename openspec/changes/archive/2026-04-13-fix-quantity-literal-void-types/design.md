## Context

Spark's `lit(null)` produces an expression of type `NullType` (`VOID`). When
used inside `struct(...)`, the resulting struct field inherits that type.
`QuantityEncoding.encodeLiteral` passes `lit(null)` for fields that have no
value in a given literal (e.g., `id`, `comparator`, `_fid`), and
`FlexiDecimalSupport.toLiteral` does the same when canonicalisation is not
possible. The struct works fine in most contexts because Spark coerces `VOID` to
the target type during operations like `concat`. However,
`to_variant_object()` — used by `variantTransformTree` inside `repeatAll()` —
requires every field to have a concrete type and rejects `VOID` with
`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.

## Goals / Non-Goals

**Goals:**

- Eliminate all `VOID`-typed fields from Quantity literal structs so they are
  compatible with `to_variant_object()` and any other schema-strict operations.
- Unblock `repeat($this)` over Quantity literal collections (issue #2588).
- Evaluate whether the fix also resolves the related "indefinite calendar
  duration union deduplication" exclusion.

**Non-Goals:**

- Changing `repeatAll()`'s type classification for Quantity (Quantity is
  correctly treated as a complex type since `<qty>.repeat(unit)` is valid
  FHIRPath).
- Fixing `QuantityEncoding.encodeNumeric` — it has similar `lit(null)` patterns
  but does not appear on a failing path today; addressing it is optional
  clean-up.

## Decisions

### 1. Cast the entire struct rather than individual fields

**Decision:** Append `.cast(dataType())` to the `toStruct(...)` result in
`encodeLiteral`, rather than casting each `lit(null)` individually.

**Rationale:** `dataType()` already declares the canonical schema for all 10
fields. A single positional struct cast resolves every `VOID` field in one
operation. This is less error-prone and less verbose than N separate casts, and
already validated empirically (see explore-mode spike).

**Alternative considered:** Cast each `lit(null)` to its target type
(`lit(null).cast(DataTypes.StringType)`, etc.). Correct but repetitive and
fragile — if a field type changes in `dataType()`, the individual casts must be
updated in lockstep.

### 2. Also fix `FlexiDecimalSupport.toLiteral` independently

**Decision:** Change the null branch from `lit(null)` to
`lit(null).cast(DATA_TYPE)`.

**Rationale:** Although the struct-level cast in `encodeLiteral` would also
resolve the `canonicalized_value` field, `FlexiDecimalSupport.toLiteral` is a
public utility used by other callers. Fixing it at source prevents the same
class of VOID issue from surfacing elsewhere.

## Risks / Trade-offs

- **Behavioural regression risk** → Low. The cast only concretises the type of
  null values; no runtime data changes. Mitigated by running the full
  `RepeatFunctionDslTest` and `YamlReferenceImplTest` suites.
- **Struct field ordering** → Spark's struct cast is positional. The field order
  in `toStruct()` and `dataType()` must match. They already do and are defined
  in the same class. Mitigated by the existing test suite.
