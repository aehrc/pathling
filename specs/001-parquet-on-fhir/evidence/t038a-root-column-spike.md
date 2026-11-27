# T038a first case: the tolerant `_extension` reference (throwaway spike)

**Result: the reference cannot be built on T009a's terms for every plan shape
T038a names.** A construction that is on T009a's terms works under Project,
Filter, an aggregate _value_, and a generator in a select. Spark refuses it in a
**grouping key**, a **sort key** and a **join condition**. The only construction
that works in a grouping key is not on T009a's terms: it catches an exception.
It also gives a silent wrong answer when the name is ambiguous.

> **Later change.** The catch that the engine adopted now falls back to a typed
> null only when no column matches the name. An ambiguous name raises Spark's
> `AMBIGUOUS_REFERENCE`, so the silent wrong answer described here no longer
> occurs. The measurements below are kept as they were taken.

## Conditions

- Spark 4.0.2 (Scala 2.13), in the `encoders` test JVM, `local[2]`, with ANSI on.
- Commit `0847e6086d`.
- Data is written to Parquet and read back:
    - present: `id`, `arr: array<struct<_fid:int,v:string>>` and
      `_extension: map<int,array<string>>`;
    - absent: the same without `_extension`.
- The source was run from `encoders/src/test/scala`, then deleted. A copy is at
  `t038a-root-column-spike.scala.txt`. The full transcript, with analyzed and
  optimised plans, is at `t038a-root-column-spike.txt`.

## What was built

Every variant is the right child of a binary
`RuntimeReplaceable with BinaryLike`:

- its left child is a `transform` lambda variable;
- its `replacement` is `GetMapValue(right, left._fid)`, a `lazy val` on a case
  class;
- it logs each forcing, with both children's `resolved` flags.

| Variant | Construction                                                                                                                                                                                                    | On T009a's terms?                                              |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| V1      | Unary `ResolveOrNull(struct(colRegex("^_extension$")), "_extension", map)`. The struct always resolves: to `struct<_extension>` where the column is present, `struct<>` where it is absent.                     | Yes                                                            |
| V2      | A custom `Star` whose `expand` gives the attribute, or `null AS _extension` typed as the map, inside `named_struct`, then `GetStructField(_, 0)`                                                                | Yes                                                            |
| V3      | A `mapChildren` catch of `INCOMPATIBLE_VIEW_SCHEMA_CHANGE` around `GetViewColumnByNameAndOrdinal`. That node is resolved only by `ColumnResolutionHelper.innerResolve`, and it throws when the name is missing. | **No**: this is the `UnresolvedFallbackIfMissingField` pattern |
| V0      | Plain `col("_extension")` (the control)                                                                                                                                                                         | n/a                                                            |

## Matrix (present / absent)

| Plan                                    | V1                                 | V2                                 | V3                                  | V0                     |
| --------------------------------------- | ---------------------------------- | ---------------------------------- | ----------------------------------- | ---------------------- |
| Project                                 | OK / OK                            | OK / OK                            | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Filter                                  | OK / OK                            | OK / OK                            | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Aggregate, **grouping key**             | **INVALID_USAGE_OF_STAR_OR_REGEX** | **INVALID_USAGE_OF_STAR_OR_REGEX** | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Aggregate, value (`agg(first(...))`)    | OK / OK                            | OK / OK                            | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Sort                                    | **INVALID_USAGE_OF_STAR_OR_REGEX** | **INVALID_USAGE_OF_STAR_OR_REGEX** | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Generate (`explode_outer` in a select)  | OK / OK                            | OK / OK                            | OK / OK                             | OK / UNRESOLVED_COLUMN |
| Join condition                          | **INVALID_USAGE_OF_STAR_OR_REGEX** | **INVALID_USAGE_OF_STAR_OR_REGEX** | **INTERNAL_ERROR (AssertionError)** | OK / UNRESOLVED_COLUMN |
| Ambiguous name (self-join, then select) | not run                            | not run                            | **null, silently**                  | AMBIGUOUS_REFERENCE    |

- **Rows.** Wherever a case ran, the present rows carried the real map value
  (`[1,[[urn:x], null]]`), which is the positive control. The absent rows were
  typed nulls.
- **Forcing.** Every forcing saw both children resolved: the left a
  `NamedLambdaVariable`, the right resolved. No premature forcing appeared in
  any shape that ran.
- **Plan.** In the absent case the optimiser folds the lookup to
  `lambdafunction(null, ...)` and reads no extra column.

## Why no star variant can reach a grouping key, sort or join

`Analyzer.ResolveReferences.doApply` (Analyzer.scala:1516-1545, 4.0.2) expands a
`Star` only in these places:

- a `Project` list;
- a `Filter` condition;
- an `Aggregate`'s **aggregate** list;
- `CollectMetrics`;
- `Unpivot`.

`CheckAnalysis.scala:406` rejects a `Star` left anywhere else. `Dataset.groupBy`
places the key in both lists, so the aggregate-list copy expands and the
grouping copy does not.

An unresolved attribute cannot help either. `innerResolve`
(ColumnResolutionHelper.scala:170-179) leaves a missing name as the
`UnresolvedAttribute` and throws nothing. It stays unresolved until
`CheckAnalysis` reports `UNRESOLVED_COLUMN`, so no expression-local hook ever
learns that the name is absent.

## Where the engine emits traversal columns today

- `select`: the evaluator's projection, and `Projection` with `inline` inside a
  select.
- `Dataset.filter`: `FhirSearchExecutor:96`, and the `PatientCompartmentService`
  filter.

None of the main code in `fhirpath`, `library-api` or `server` puts a FHIRPath
column into a grouping key, a sort key or a join condition. The one join, in
`PatientCompartmentService`, is on `id`.

**But the public API hands FHIRPath columns to callers.** It does so through
`PathlingContext.fhirPathToColumn` and `searchToColumn`
(`PathlingContext.java:747` and `:783`), and their Python and R equivalents
(`lib/python/pathling/context.py`, `lib/R/R/context.R`). A user can group, sort
or join on those columns. So option 1's restriction would be a user-visible
limitation on previous-layout data wherever the extension branch is emitted.

## Decision 74's two items, separately

- **Item 2, the binary form: passes.** Evidence: V0 on present data, where the
  right child is a plain column and the left child a lambda variable. The node
  analysed and gave correct rows under Project, Filter, the grouping key, the
  aggregate value, Sort and Generate, and every forcing saw both children
  resolved. The grouping key was the shape T038m left open.
- **Item 1, the tolerant reference: fails**, as described above.

## Options for the owner (not chosen)

1. **Accept V1**, the star-based reference, on T009a's terms. Record that the
   traversal expression is not usable in grouping, sort or join keys over
   previous-layout data, and that a caller must project first. Every present
   engine site is covered. T038a's "Aggregate (groupBy)" is then met as an
   aggregate value, not as a key.
2. **A construction-time schema check at the traversal site**, choosing
   `col("_extension")` or a typed-null literal from the dataset's schema. This
   works in every plan shape, but it is not on T009a's terms.
3. **An injected analyzer rule**, which replaces an unresolved tolerant
   reference with a typed null after `ResolveReferences` reaches its fixed
   point. It is general, but it needs `SparkSessionExtensions`, which Pathling
   cannot guarantee on a user-supplied session.
4. **V3, the catch**, is not recommended: it answers null silently where the
   name is ambiguous, and it crashes in join conditions.
