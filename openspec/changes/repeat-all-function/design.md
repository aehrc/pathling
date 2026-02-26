## Context

Pathling encodes FHIR resources into Spark DataFrames, truncating recursive
structures (e.g., `Questionnaire.item`) at `maxNestingLevel` (default: 3). This
means items at different depths have different struct schemas — deeper levels
have fewer fields as recursive children are progressively removed.

The `encoders` module already provides `UnresolvedTransformTree`, a custom
Catalyst expression that implements depth-limited recursive tree traversal at
query planning time. It is used by the SQL-on-FHIR `RepeatSelection` to
implement the `repeat` clause in view definitions. `ValueFunctions.transformTree`
provides the Java bridge to this expression.

`UnresolvedTransformTree` works by concatenating arrays extracted at each nesting
level via Spark's `Concat`. This requires all arrays to have compatible element
types. In `RepeatSelection`, the extractor applies a user-defined projection
(e.g., extract `linkId`, `text`) that produces a consistent flat schema at every
level. For FHIRPath `repeatAll(item)`, the projection IS the recursive
navigation — `item` returns the child items, whose schemas differ by depth.

FHIRPath functions are registered via `@FhirPathFunction`-annotated static
methods. Scoped functions that accept projection expressions use the
`CollectionTransform` parameter type, which the resolver wraps around the
FHIRPath argument expression with `$this` binding.

## Goals / Non-Goals

**Goals:**

- Implement `repeatAll()` as a FHIRPath function that recursively traverses
  hierarchical FHIR structures and collects all results without deduplication.
- Reuse the existing `UnresolvedTransformTree` infrastructure for depth-limited
  recursive traversal.
- Solve the schema divergence problem so items from different nesting levels can
  be concatenated into a single output collection.
- Make the function available through standard FHIRPath evaluation with no API
  changes.

**Non-Goals:**

- Implementing `repeat()` (requires element-wise deduplication, which is a
  separate and harder problem in Spark's columnar model).
- Supporting unbounded recursion — same-type traversal depth is always limited.
- Optimising for cyclic data — `repeatAll()` does not guard against cycles, per
  the FHIRPath specification. Same-type depth limiting provides practical
  termination.

## Decisions

### Decision 1: Use Variant round-trip to solve schema divergence

At each nesting level, items extracted by the projection have different struct
schemas due to `maxNestingLevel` truncation. Spark's `Concat` requires arrays
with identical element types and its type coercion only handles structs with the
same number of fields.

**Chosen approach:** Use Spark 4's `VariantType` as a schema-free intermediate
representation. At each level:

1. The extractor evaluates the projection and converts results to Variant via
   `to_variant_object()`, producing `Array[Variant]`.
2. `Concat` merges all `Array[Variant]` arrays across levels (uniform type, no
   coercion needed).
3. The final result is converted back via `variant_get(v, '$', targetSchema)`
   where `targetSchema` is the fullest schema (from level 0). Missing fields at
   deeper levels are filled with null.

**Alternatives considered:**

- **Custom `UnresolvedPadToSchema` Catalyst expression**: A new expression that
  recursively aligns structs to a target schema by adding null-valued fields.
  Full type safety with no serialization overhead, but introduces a new Catalyst
  expression to maintain and requires recursive struct alignment logic. The
  target schema must be determined and propagated into the expression.

- **Lazy collection with deferred extraction**: Defer `UnresolvedTransformTree`
  construction until a downstream expression provides a uniform schema. Avoids
  schema divergence entirely but changes `repeatAll()` semantics —
  `repeatAll(item)` alone would not be a valid terminal expression. Introduces
  complexity in chaining and composability with aggregate operations.

- **Fixed-depth unrolling with `transform`/`flatten`**: Build explicit nested
  chains without `UnresolvedTransformTree`. Easy to debug but hard to generalise,
  verbose, and duplicates logic that `UnresolvedTransformTree` already solves.

- **Drop recursive fields (normalise to shallowest schema)**: Strip
  self-referential fields from output. Trivially compatible but loses structural
  information users need for downstream processing.

**Rationale:** The Variant approach requires no new Catalyst expressions, uses
built-in Spark 4 functions, and preserves full spec conformance — items are
returned with their complete structure. The serialization overhead is acceptable
given the alternative complexity.

### Decision 2: Add `repeatAll` method on `Collection`

The integration point is a new `Collection.repeatAll(CollectionTransform, int)`
method that:

1. Applies the transform to derive the result type metadata.
2. Converts the `CollectionTransform` to a `ColumnTransform` via
   `toColumnTransformation()`.
3. Pre-computes the level-0 result by applying the column transform to the
   input and normalizing to array form via `plural()`, producing
   `Array[ItemStruct]`.
4. Passes the level-0 result to `ValueFunctions.transformTree()` as the starting
   node, with:
    - **Extractor**: a vectorised `to_variant_object` conversion —
      `transform(node, to_variant_object)` — which converts each element of the
      current array to Variant, producing `Array[Variant]`.
    - **Traversal**: the same column transform (projection expression), which
      navigates from items to their children at each level.
5. Unwraps the final `Array[Variant]` result back to the target schema via
   `variantUnwrap` using the level-0 expression as the schema reference.
6. Returns a new `Collection` with the result column and appropriate type
   metadata.

This mirrors the `RepeatSelection` pattern where the extractor captures "what to
collect" and the traversals capture "where to go next". For `repeatAll`, "what
to collect" is the items themselves (as Variant for schema compatibility), and
"where to go next" is the same projection expression that produced level 0.

**Rationale:** Reusing `UnresolvedTransformTree` gives us same-type depth
limiting for free — the depth counter only decrements when traversing to a node
of the same type, matching the SQL on FHIR `repeat` clause behaviour. The
extractor is a simple `to_variant_object` mapping with no need to compose the
projection into it, since the projection lives entirely in the traversals.

### Decision 3: Register as a standard FHIRPath function

`repeatAll` is registered as a `@FhirPathFunction`-annotated static method in
the filtering and projection function group (alongside `select`, `where`,
`repeat`). The method signature accepts `Collection` (input) and
`CollectionTransform` (projection), matching the existing pattern for scoped
functions.

**Rationale:** Consistent with how `select()` is registered. No changes needed
to the function resolution or parameter binding infrastructure since
`CollectionTransform` is already a supported parameter type.

### Decision 4: Same-type depth limiting matching the SQL on FHIR repeat clause

Traversal always navigates to the deepest level possible regardless of the
encoding's maximum nesting level. `UnresolvedTransformTree` only decrements its
depth counter when traversing to a node of the same type as its parent — paths
through different types (e.g., Item → Answer → Item) traverse freely without
consuming depth budget. This matches the approach used by `RepeatSelection` for
the SQL on FHIR `repeat` clause.

The depth limit for same-type recursion is hardcoded to 10, matching the default
`maxUnboundTraversalDepth` used by `RepeatSelection`. In the first instance this
value is not configurable.

**Rationale:** The encoding's `maxNestingLevel` controls how deep recursive
structures are materialised in the schema, but `UnresolvedTransformTree` already
handles schema truncation gracefully — when a field is not found during
traversal it returns an empty array rather than failing. Tying the traversal
depth to `maxNestingLevel` would be unnecessarily restrictive for structures
where the traversal expression crosses different types before recurring. Using
the same approach as the `repeat` clause ensures consistent behaviour.

### Decision 5: Normalize projection results to arrays via `plural()`

`variantTransformTree` requires array-typed input at every level — its extractor
calls `transform(node, to_variant_object)` which is a Spark function that
operates on arrays. When the projection expression produces a singular value
(e.g., `Patient.repeatAll(gender)` returns a `STRING`, not `ARRAY<STRING>`), the
pipeline fails.

**Chosen approach:** A two-part solution:

1. **`plural()` normalization**: Apply `plural()` to both the level-0 result and
   the inner traversal result to ensure they are always represented as arrays.
   The `plural()` method on `ColumnRepresentation` handles this via
   `vectorize()`: array input passes through (null becomes empty array), scalar
   input is wrapped in `array(scalar)` (null becomes empty array).

2. **Primitive type short-circuit**: Check whether the projection produces a
   primitive (non-struct) FhirPathType. If so, return the level-0 result
   directly without invoking tree traversal. Primitives are leaf types with no
   children to recurse into, so the result is always just the projected values.
   This avoids two issues: `to_variant_object()` cannot convert primitives to
   Variant, and `UnresolvedTransformTree` cannot navigate fields on primitive
   array elements.

The primitive check uses the `Collection`'s FhirPathType metadata:

    resultTemplate.getType()
        .map(t -> !(t.getSqlDataType() instanceof StructType))
        .orElse(false)

Types with no FhirPathType mapping (e.g., CodeableConcept, HumanName,
QuestionnaireItem) default to the Variant path, which is correct since they are
always represented as structs.

**Rationale:** `plural()` is the existing mechanism used throughout the codebase
(e.g., in `Collection.filter()`, `Collection.aggregate()`) to normalize
singular-vs-collection representation. The primitive short-circuit is necessary
because `to_variant_object()` only works on structs/maps (Spark's
`StructsToVariant` expression), and `UnresolvedTransformTree` interprets field
access on primitives as element access rather than producing an empty result.
For complex (struct) types, the full `variantTransformTree` path handles schema
divergence across nesting levels as before.

## Risks / Trade-offs

**Variant serialization overhead** → The round-trip through
`to_variant_object()` and `variant_get()` involves binary
serialization/deserialization at each nesting level. For most FHIR resources with
modest nesting (2-4 levels), this overhead is expected to be negligible. If
profiling reveals issues, the `UnresolvedPadToSchema` approach can be
implemented as an optimisation without changing the public API.

**Variant type fidelity** → Spark's Variant documentation notes it "does not yet
contain lossless equivalents" for all types. Complex FHIR types (decimals with
specific precision, dates, nested extensions) need validation. A spike test with
a `Questionnaire` containing nested items should verify round-trip fidelity
before full implementation.

**Spark 4 dependency** → `VariantType` and associated functions are Spark 4
features. Pathling already targets Spark 4, so this is not a new dependency, but
it does tie this implementation to the Spark 4+ line.

**No cycle detection** → Per the FHIRPath spec, `repeatAll()` does not guard
against cycles. In Pathling's encoded data, cycles cannot exist (the schema is
truncated), so this is not a practical concern. The same-type depth limit
provides an additional safety net.

## Resolved Questions

1. **Variant fidelity validation**: Spike tests (tasks 1.1 and 1.2) confirmed
   that the `to_variant_object` → `variant_get` round-trip preserves FHIR data
   types faithfully, including nested structs with missing fields at deeper
   levels which are correctly filled with null.

2. **Target schema determination**: Resolved by introducing
   `UnresolvedVariantUnwrap` — a Catalyst expression that accepts the
   `Array[Variant]` result and a schema reference expression (the level-0 array).
   Schema extraction is deferred to Catalyst analysis time, when the level-0
   expression has resolved and its element type can be read directly. No
   pre-computation of the target schema is needed.

3. **Non-recursive projection**: No error is produced. If `repeatAll` is called
   with a projection that does not navigate into a recursive structure (e.g.,
   `Patient.repeatAll(name)`), the function silently returns the same result as
   `select()` — one iteration producing results, followed by termination when
   the next iteration yields an empty collection. This is consistent with how
   FHIRPath functions generally do not validate the semantic meaningfulness of
   their arguments.
