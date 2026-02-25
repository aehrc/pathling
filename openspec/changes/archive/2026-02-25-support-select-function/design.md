## Context

Pathling's FHIRPath engine implements functions as annotated static methods in provider classes (e.g., `FilteringAndProjectionFunctions`). The `where` function already demonstrates the pattern for functions that take an expression parameter: it receives a `CollectionTransform` that wraps the parsed FHIRPath expression, converts it to a `ColumnTransform`, and applies it via `Collection.filter()`.

The `select` function is closely related to `where` - both iterate over elements and evaluate an expression per element. The difference is that `where` uses the expression result as a boolean filter, while `select` uses it as a projection (the expression result replaces the element).

## Goals / Non-Goals

**Goals:**

- Implement the FHIRPath `select(projection: expression) : collection` function per the specification
- Follow existing patterns established by `where` for expression-parameter functions
- Support flattening of results (collections produced by the projection are merged into the output)

**Non-Goals:**

- Supporting `$this` and `$index` special variables within `select` expressions (these may already work through existing infrastructure, but are not a specific goal)
- Optimising for deeply nested projections

## Decisions

### 1. Implement as a new method in `FilteringAndProjectionFunctions`

**Rationale:** The FHIRPath spec groups `select` under "Filtering and projection" alongside `where`. The existing class already handles this category. Adding `select` here maintains the logical grouping.

**Alternative considered:** Creating a separate provider class. Rejected because it would fragment closely related functions.

### 2. Use `CollectionTransform.toColumnTransformation()` for expression application

**Rationale:** The `toColumnTransformation` method already handles the pattern of applying an expression to each element by creating a copy of the input with the element's column, evaluating the expression, and returning the resulting column. This is exactly what `select` needs - the only difference from `where` is what we do with the result column (use it directly vs. use it as a filter predicate).

### 3. Add a `project` method to `Collection`

**Rationale:** `Collection.filter()` handles the `where` semantics (filtering by boolean). We need a parallel method for `select` semantics (replacing the element with the expression result). The `project` method will apply a `ColumnTransform` and return a new collection with the projected values. Unlike `filter`, the result collection's type information will come from the expression result rather than the input.

**Alternative considered:** Reusing `Collection.map()` directly. However, `map` preserves the input collection's type metadata, while `select` should adopt the type of the projection expression result.

## Risks / Trade-offs

- **[Type information loss]** → The projection expression may produce a different type than the input collection. The `CollectionTransform` approach captures the full result including type, so we should extract type info from the expression result.
- **[Flattening semantics]** → In the columnar Spark model, "flattening" of nested arrays from projection results may require special handling. However, the existing column representation already handles array semantics, so this should work naturally.
