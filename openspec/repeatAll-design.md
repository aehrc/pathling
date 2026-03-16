# Design: FHIRPath `repeatAll()` Function Implementation

## Specification

### `repeatAll(projection: ($this) => collection) : collection`

> Note: This function is Standard for Trial Use (STU) in the FHIRPath specification.

A version of `repeat()` that allows duplicate items in the output collection.
Unlike `repeat()`, this function does not check whether items are already present
in the output collection before adding them.

**Algorithm:**

1. Add all items in the input collection to an **input queue**.
2. Remove the first item from the input queue and evaluate the projection
   expression against it.
3. Add the results to the **output collection** and also to a **new iteration
   queue**, regardless of whether they already exist in either collection.
4. Once all items in the input queue have been processed, replace the input
   queue with the new iteration queue.
5. Repeat from step 2 until the input queue is empty (an iteration produces no
   new results).

The order of items returned is undefined. The meaning of `$index` is undefined
and not set.

### Comparison with `repeat()`

| Aspect          | `repeat()`                                            | `repeatAll()`                             |
| --------------- | ----------------------------------------------------- | ----------------------------------------- |
| Deduplication   | Yes — only adds items not already in output (via `=`) | No — adds all items regardless            |
| Termination     | Queue drains because duplicates are never re-enqueued | Iteration wave produces zero new results  |
| Performance     | Slower — equality comparison on every result          | Faster — skips equality checks            |
| Cyclic safety   | Safe (dedup prevents infinite loops)                  | Could infinite-loop on cyclic data        |
| Iteration model | Single work queue, item-by-item                       | Wave-based: process full queue, then swap |
| Spec status     | Normative                                             | STU                                       |

### Why `repeatAll()` first

We implement `repeatAll()` before `repeat()` because it does not require
element-wise equality/deduplication, which is impractical in a Spark columnar
context with complex nested FHIR types.

## Key Concerns

### 1. Expressing recursive traversal in Spark SQL

The iterative reference algorithm (work queue with waves) cannot be directly
implemented in Spark SQL — there are no recursive CTEs or iterative operators.
The traversal must be expressed as a single expression that is unrolled to a
fixed depth at query planning time.

### 2. Schema divergence across nesting levels

When FHIR data is encoded into Spark, recursive structures (e.g.,
`Questionnaire.item`) are truncated at `maxNestingLevel`. This means:

```
Level 0: Struct[linkId, text, type, item: Array[Struct[linkId, text, type, item: Array[...]]]]
Level 1: Struct[linkId, text, type, item: Array[Struct[linkId, text, type]]]
Level N: Struct[linkId, text, type]  (recursive fields truncated)
```

Items extracted at different nesting levels have **different struct schemas**.
Spark's `Concat` requires arrays with identical element types — its type
coercion only handles structs with the **same number of fields**. Structs with
different field counts fail coercion entirely.

The recursive fields should be **preserved** in the output (not stripped),
meaning items in the result collection should retain their child elements where
they exist.

## Primary Solution: `UnresolvedTransformTree` with Schema Padding

### Reusing existing infrastructure

The `encoders` module already contains `UnresolvedTransformTree` — a custom
Spark Catalyst expression that implements depth-limited recursive tree traversal
at query planning time. It is used by the SQL-on-FHIR `RepeatSelection` to
implement the `repeat` clause in view definitions.

`UnresolvedTransformTree` works by:

1. Extracting a value from the current node (via an `extractor` function).
2. Recursively traversing to child nodes (via `traversals` functions).
3. Concatenating extracted values from all levels via Spark's `Concat`.
4. Depth-limiting same-type recursion while allowing traversal through different
   types without decrementing.
5. Returning `CreateArray(Seq.empty)` (type `ArrayType(NullType)`) when recursion
   terminates — compatible with any array type.

### Why it works for SQL-on-FHIR but not directly for FHIRPath

In `RepeatSelection`, the extractor applies a **projection** (e.g., extract
`linkId`, `text`) — producing a consistent flat schema at every level. The items
themselves are never collected; only projected values are.

For FHIRPath `repeatAll(item)`, the projection IS the recursive navigation —
`item` returns the child items, whose schemas differ by depth. The items
themselves must be collected with their recursive fields intact.

### The schema padding approach

To make the output types compatible across levels, each extracted item must be
**padded** to match a single target schema (the fullest schema, from level 0).
Missing fields at deeper levels are filled with null or empty arrays.

The extractor in `transformTree` would be wrapped with a padding operation:

```
extractor(node) → Array[Struct_at_this_level]
    → pad each element to targetSchema
    → Array[Struct_target]  (consistent type at every level)
```

This can be implemented in two ways:

### Option A: Custom Catalyst expression (`UnresolvedPadToSchema`)

Create a new Catalyst expression that takes a target `StructType` and
recursively aligns a struct to that schema:

```scala
case class UnresolvedPadToSchema(value: Expression, targetSchema: StructType)
    extends Expression with UnevaluableCopy with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {
    val newValue = f(value)
    if (newValue.resolved) {
      val actual = newValue.dataType.asInstanceOf[StructType]
      val paddedFields = targetSchema.fields.map { targetField =>
        if (actual.fieldNames.contains(targetField.name))
          padField(GetStructField(newValue, actual.fieldIndex(targetField.name)),
                   targetField.dataType)
        else
          Literal(null, targetField.dataType)
      }
      CreateNamedStruct(interleaveWithNames(paddedFields, targetSchema))
    } else copy(value = newValue)
  }
}
```

**Pros:**

- Full type safety — operates on native Spark types throughout.
- No serialization overhead.
- Reusable for other schema-mismatch problems in the codebase.

**Cons:**

- New Catalyst expression to maintain.
- Must handle recursive struct alignment (structs within structs).
- Target schema must be determined and propagated into the expression.

### Option B: Variant round-trip

Use Spark 4's `VariantType` as a schema-free intermediate:

```
At each level:
  items → transform(_, to_variant_object) → Array[Variant]

Concat all Array[Variant] arrays  (same type — no coercion needed)

Convert back:
  transform(result, v -> variant_get(v, '$', targetSchema)) → Array[TargetStruct]
```

`variant_get` with a target `StructType` extracts matching fields and fills
missing ones with null.

**Pros:**

- No custom Catalyst expressions — uses built-in Spark 4 functions.
- Simpler implementation.

**Cons:**

- Performance overhead from binary serialization/deserialization at each level.
- Spark explicitly warns that Variant "does not yet contain lossless equivalents"
  for structs/maps — direct `cast(struct as variant)` is blocked, requiring
  `to_variant_object()`.
- Type fidelity risk for complex FHIR types (decimals, dates, nested
  extensions) — needs validation via a spike test.
- Dependency on Variant stability across Spark versions.

### Integration with `Collection`

Regardless of padding approach, the integration point is a new method on
`Collection`:

```java
public Collection repeatAll(CollectionTransform transform, int maxDepth) {
    Collection resultTemplate = transform.apply(this);
    ColumnTransform columnTransform = transform.toColumnTransformation(this);
    // targetSchema derived from resultTemplate

    Column result = ValueFunctions.transformTree(
        getColumn().getValue(),
        c -> pad(columnTransform.apply(getColumn().copyOf(c)).getValue(), targetSchema),
        List.of(c -> columnTransform.apply(getColumn().copyOf(c)).getValue()),
        maxDepth);

    return resultTemplate.copyWith(new DefaultRepresentation(result).flatten());
}
```

`toColumnTransformation()` converts the `CollectionTransform` (FHIRPath
expression operating on Collections) into a `ColumnTransform` (operating on raw
columns) by wrapping each column in a Collection with preserved type metadata.
This is schema-agnostic and works at any nesting depth.

## Other Approaches Considered

### Delegate to `Collection.project()` (single-level only)

Implement `repeatAll()` identically to `select()` — one level of projection
with no recursion.

**Pros:** Trivial implementation. No schema issues.
**Cons:** Not actually `repeatAll()` — only traverses one level. Users would
need to chain calls manually for deeper structures. Does not match the spec
semantics.

### Recursive DataFrame operations

Use `Dataset.union` in a loop, materializing intermediate results until a fixed
point is reached.

**Pros:** True iterative semantics matching the spec.
**Cons:** Requires materializing intermediate DataFrames. Incompatible with
Pathling's columnar evaluation model where expressions produce columns, not
DataFrames. Expensive for deep structures.

### Fixed-depth unrolling with `transform`/`flatten`

Build an explicit chain of nested `transform` + `flatten` calls without using
`UnresolvedTransformTree`:

```
concat(
    transform(input, x -> x.item),
    flatten(transform(input, x -> transform(x.item, y -> y.item))),
    ...
)
```

**Pros:** No custom Catalyst expressions. Explicit and easy to debug.
**Cons:** Hard to generalise for arbitrary depth. Verbose. Doesn't handle the
schema divergence problem (same issue with Concat). Duplicates logic that
`UnresolvedTransformTree` already solves.

### Drop recursive fields from output (normalise to shallowest schema)

Strip all self-referential fields from extracted items so every level produces
the same flat schema.

**Pros:** Guarantees schema compatibility trivially.
**Cons:** User explicitly needs recursive fields preserved in the output. Loses
structural information that may be needed for downstream processing.

## Alternative Solution: Lazy Collection with Deferred Extraction

### Concept

Instead of solving the schema divergence at the Spark expression level (via
padding or Variant), avoid the problem entirely by **deferring the construction
of `UnresolvedTransformTree`** until we know both the traversal and a downstream
expression that produces a uniform schema.

`repeatAll(item)` would not immediately build a Spark expression. Instead, it
returns a **lazy (deferred) Collection** — analogous to how
`ChoiceElementCollection` works — that captures the traversal intent but cannot
be evaluated on its own. When a subsequent FHIRPath operation is applied to this
collection, that operation becomes the **extractor** for `UnresolvedTransformTree`,
and the full expression is materialised at that point.

### How it works

Given `Questionnaire.repeatAll(item).linkId`:

1. `repeatAll(item)` returns a `RepeatAllCollection` (lazy) that records:
    - The input collection (`Questionnaire`)
    - The traversal function (`item` navigation)
    - The max depth
2. `.linkId` is applied to the lazy collection. The lazy collection intercepts
   this and uses it as the extractor.
3. Now both pieces are available. The lazy collection builds
   `UnresolvedTransformTree` with:
    - **Traversal**: `item` navigation (from step 1)
    - **Extractor**: `.linkId` projection (from step 2) — produces a consistent
      `StringType` at every nesting level
4. The result is a concrete `Collection` of linkId values from all levels.

### Why this avoids schema divergence

The extractor is a **user-supplied downstream expression** that projects specific
fields, producing a uniform output type at every level — exactly the pattern
that already works in `RepeatSelection` for SQL-on-FHIR. The items themselves
(with their divergent schemas) are never collected; only the extracted values
are.

### Precedent: `ChoiceElementCollection`

Pathling already uses this pattern for FHIR choice types. A
`ChoiceElementCollection` cannot be directly evaluated — it requires a
subsequent type-specific path step (e.g., `.ofType(Quantity).value`) to resolve
into a concrete column. The same deferred-resolution pattern applies here.

### Error on direct evaluation

If `repeatAll(item)` is used as a terminal expression (no downstream operation),
it should produce an error, consistent with other lazy collections. For example:

```
Questionnaire.repeatAll(item)  → ERROR: repeatAll requires a subsequent
                                  expression to determine the extraction schema
```

### Chaining and extractor determination

For a chain like `Questionnaire.repeatAll(item).where(type = 'group').linkId`:

The lazy collection must determine which downstream operations become the
extractor. Conceptually, the **entire chain** after `repeatAll()` forms the
extractor — each element at every nesting level is evaluated through
`.where(type = 'group').linkId`. The first operation that touches the lazy
collection (`.where(...)`) triggers resolution, composing itself and all
subsequent operations into the extractor.

In practice, this means the lazy collection needs to intercept the next
operation and wrap it such that the full downstream chain is captured. The
precise mechanism for this requires further investigation — it may involve:

- The lazy collection returning a modified context that composes operations
  until a terminal expression is reached.
- Or, more practically, having the lazy collection absorb only the **immediate
  next** operation as the extractor, and letting the rest of the chain operate
  on the resulting concrete collection.

### Pros

- **Eliminates schema divergence entirely** — no padding, no Variant, no custom
  Catalyst expressions for schema alignment.
- **Mirrors the SQL-on-FHIR pattern** — the extractor always produces a uniform
  type, which is the proven approach in `RepeatSelection`.
- **Leverages existing infrastructure** — `UnresolvedTransformTree` is used
  as-is without modification.
- **Clean separation** of traversal (what to recurse into) from extraction
  (what to collect).

### Cons

- **Cannot return the items themselves** — the result is always a projection
  from the items, not the items. Users cannot write
  `repeatAll(item).first().answer` because the items are never materialised as
  a collection. This is a fundamental semantic difference from the spec, which
  defines `repeatAll()` as returning a collection of the traversed elements.
- **Increased complexity in the Collection abstraction** — introduces a new
  deferred collection type that must intercept and compose downstream
  operations.
- **Chaining ambiguity** — determining where the "extractor" ends and normal
  collection operations begin is non-trivial, especially with complex chains.

### Open Concerns

1. **Recursive downstream traversal**: If the downstream expression itself
   navigates into a recursive field (e.g., `repeatAll(item).item`), the
   extractor still produces divergent schemas across levels — the same problem
   resurfaces. The lazy approach only helps when the downstream expression
   reaches leaf fields or non-recursive fields.

2. **Chaining `repeatAll` after `repeatAll`**: An expression like
   `Questionnaire.repeatAll(item).repeatAll(answer)` would produce a lazy
   collection wrapping another lazy collection. The resolution order and
   composition of extractors becomes complex. Does the inner `repeatAll`
   resolve first, or does it also defer?

3. **Composability with aggregate operations**: Operations like `count()`,
   `exists()`, `first()` applied to the lazy collection need to trigger
   resolution. For `count()`, the extractor could be a trivial identity that
   just counts elements — but this brings back the schema divergence problem
   since we'd be counting items with different schemas. For `exists()`, it
   could short-circuit without needing a schema-uniform array.

4. **Spec conformance**: The FHIRPath spec defines `repeatAll()` as returning
   a collection of the projected elements. A lazy collection that requires
   further resolution changes the semantics — `repeatAll(item)` alone should
   be a valid expression that returns items. This approach trades spec
   conformance for implementation feasibility.

5. **Interaction with `select()`**: Could `repeatAll(item).select(linkId)` be
   the idiomatic pattern? If so, `select()` naturally provides the extractor.
   But this still leaves open the question of what happens without `select()`.

## Open Questions

1. **Max depth default**: What should the default `maxDepth` be for FHIRPath
   `repeatAll()`? Should it match the encoder's `maxNestingLevel`, or be
   independently configurable?

2. **Variant fidelity**: Does `to_variant_object` → `variant_get` round-trip
   preserve all FHIR data types faithfully? A spike test with a
   `Questionnaire` containing nested `item` elements would answer this.

3. **`repeat()` path**: Once `repeatAll()` is implemented, `repeat()` would
   need the same recursive machinery plus element-wise deduplication. Should
   deduplication be deferred entirely, or approximated (e.g., by hash)?

4. **Schema padding vs lazy collection**: The schema padding approach (primary
   solution) preserves full spec conformance but requires solving the schema
   divergence problem. The lazy collection approach avoids schema divergence
   but changes the function's semantics. Which trade-off is preferable?
