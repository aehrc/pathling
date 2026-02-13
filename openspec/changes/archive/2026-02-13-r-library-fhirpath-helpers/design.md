## Context

The R library provides `pc_fhirpath_to_column` and `pc_search_to_column`, which return raw JVM Column objects. Using these with sparklyr requires manual `j_invoke("filter", col)` / `j_invoke("withColumn", name, col)` calls and `sdf_register()`. Spark's `Dataset.select(Column*)` cannot be called at all because Scala varargs are incompatible with sparklyr's `j_invoke`. The terminology functions (`tx_*`) avoid this by returning `rlang::expr()` objects that work with dplyr verbs, but the FHIRPath/search column functions build expression trees on the JVM and cannot use the same approach.

## Goals / Non-goals

**Goals:**

- Provide DataFrame-in/DataFrame-out helper functions that integrate FHIRPath and search columns into standard dplyr/sparklyr pipelines.
- Follow sparklyr naming conventions (`sdf_with_*` pattern for adding columns, DataFrame as first argument).

**Non-goals:**

- Changing the existing `pc_fhirpath_to_column` or `pc_search_to_column` API.
- Making JVM Column objects work natively with dplyr verbs (this is a sparklyr limitation).

## Decisions

**1. Use `Dataset.withColumn(String, Column)` for adding columns.**

The `withColumn` method has a clear two-argument JVM signature that `j_invoke` can resolve without issues. This is simpler than converting R lists to Scala Seq objects via `scala.Predef.wrapRefArray()`. Users chain multiple `pathling_with_column` calls for multiple columns, which is idiomatic R pipe style.

**2. Use `Dataset.filter(Column)` for filtering.**

This already works with `j_invoke` (single-argument overload). The helper wraps it with `sdf_register()` to return a `tbl_spark`.

**3. `pathling_filter` accepts both FHIRPath and search expressions via a `type` parameter.**

Rather than creating separate functions for FHIRPath vs search filtering, a single `pathling_filter` function uses a `type` parameter defaulting to `"fhirpath"`. This keeps the API surface small.

**4. Both functions accept `pc` as the second argument (not first).**

The DataFrame is the first argument to enable piping. The PathlingContext, resource type, and expression follow. This matches sparklyr's `sdf_with_*` convention where the data object comes first.

## Risks / Trade-offs

**`withColumn` is deprecated in Spark 4.x** → The Spark 4 deprecation notice recommends `select` instead, but `select` has the varargs problem. `withColumn` still works and is the pragmatic choice. If Spark removes it in a future version, we can switch to the `wrapRefArray` approach at that point.

**Chaining `withColumn` for multiple columns** → Each call creates a new DataFrame object. For a small number of columns this is fine. Not a concern for typical usage.
