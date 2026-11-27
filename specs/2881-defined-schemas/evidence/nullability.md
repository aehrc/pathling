# Spike: exact output nullability, public API only

Decision taken on this evidence: research R15 (owner, 2026-11-27). The given schema's nullability
is ignored and each source's own nullability passes through, so no narrowing check is built.

Stored flags pass through the projection of a stored read too, including in subtrees it rebuilds,
except that a `NOT NULL` field inside a nullable struct that is rebuilt comes out nullable. This
spike measured the native readers and its own `Conform` projection, not that projection; the
guarantee for it is established by the nullability cases of `ProjectionTest` in `io`.

Issue #2881. Spark 4.1.0, Delta 4.2.0 (the runtime's `pathling.sparkVersion` and
`pathling.deltaVersion`), Java 21, `local[2]`, ANSI on. First run 2026-11-27 on Spark 4.0.2 and
Delta 4.0.1; re-run the same day on the runtime versions, with no behavioural difference (only query
ids, temporary paths, a stack-trace line number and the order of an unordered `collect` changed). The
harness and captured output are in [`nullability/`](nullability/) (`./run.sh q1|q2|q2s|q3|q4|q5|q6|all`, output in
`nullability/results/out-*.txt`). [M] = measured, [I] = inferred.

Defined schema `NN`: `id string NOT NULL`, `s struct<x int NOT NULL>` (s nullable),
`arr array<int, containsNull=false>`, `m map<string,string, valueContainsNull=false>`. Rows: good;
bad0 null `id`; bad1 null `s.x` with `s` present; bad2 `s` absent (valid); bad3 null array element;
bad4 null map value.

## 1. Native Spark behaviour (out-q1)

- `read().schema(NN).json/.parquet`, `from_json(NN)` and streaming JSON: output all nullable at every
  depth; nulls pass through [M].
- Delta read with any user schema: `DeltaDataSource source does not support user-specified schema.` [M]
- `createDataFrame(rows, NN)`: keeps the flags; null `id` gives an `INTERNAL_ERROR` NPE without the
  field name; a null array element silently becomes 0 [M]. Via `javaRDD`: leaf-only or unnamed errors,
  batch only [M].

## 2. Narrowing (out-q2, out-q2s, out-q6)

Not usable [M]: `coalesce(c, raise_error)`, `when(isNull, raise_error).otherwise(c)`, `nvl` (raise but
stay nullable); `Dataset.to(NN)` (`NULLABLE_COLUMN_OR_FIELD`, even with no nulls); `cast` to
`containsNull=false` (`CAST_WITHOUT_SUGGESTION`).

Works [M]: `coalesce(when(c.isNull(), raise_error(lit(msg))).otherwise(v), sentinel)` with a typed
non-null literal sentinel that is never returned. Output is `NOT NULL`; the optimised plan keeps the
check.

- Nested: parent becomes `when(s.isNotNull(), struct(<checked x>))`; bad2 passes.
- Arrays: `transform(arr, x -> <checked x>)` gives `containsNull=false`.
- Maps: `transform_values(m, (k, v) -> <checked v>)` gives `valueContainsNull=false`.
- bad0, bad1, bad3, bad4 fail at the action with `[USER_RAISED_EXCEPTION] NOT NULL field
'id'|'s.x'|'arr[]'|'m{}' is null`. Output schema equals `NN` exactly; `.to(NN)` accepted after.
- Streaming: the Dataset and each `foreachBatch` batch report `NN`; a null fails the query with
  `STREAM_FAILED` wrapping the same message.
- Never at the read call. A write fails with `TASK_WRITE_FAILED` caused by `USER_RAISED_EXCEPTION`.

**Caveat [M]:** the check runs only where its column is evaluated. `Narrow(bad0).select("s")`
succeeds; `count()` returns 2; `filter(id isNull)` returns **no rows and no error** (the optimiser
trusts `NOT NULL` and folds `isNull` to false).

Row-level guard filter (`Guard.java`), applied to the still-nullable input: not pruned by `count()` or
`select("s")`, fails with the right path, bad2 passes, works on streams [M]. A downstream
`filter(id isNull)` on the narrowed output still removes it [M]. Costs a second evaluation of the
nested checks [I].

## 3. Widening (out-q3, out-q2s)

- `Dataset.to(NULLABLE)`: widens only nested struct fields [M].
- `when(c.isNotNull(), v)` at every depth: fully widened, batch and streaming [M].
- `struct(col("*")).cast(NULLABLE)` then `select("r.*")`: fully widened [M].

## 4. Delta writes (out-q4)

- NN-marked data appended into a nullable table: succeeds; the table stays nullable [M].
- A table created from an NN-marked dataset (path, `saveAsTable`, streaming): **all nullable** [M].
  Only `DeltaTable.create().addColumns(NN)` keeps the flags. SQL DDL cannot express
  `ARRAY<INT NOT NULL>` [M].
- A null `id` or `s.x` appended into a NOT NULL table: `[DELTA_NOT_NULL_CONSTRAINT_VIOLATED]`,
  batch and streaming [M].
- bad2 (`s` absent, valid) is rejected with `column: s.x` [M]: Delta's nested NOT NULL is stricter
  than the spec.
- A null array element or map value into a `containsNull=false` / `valueContainsNull=false` table is
  **accepted silently**; `[1,null]` reads back as `[1,1]`, and the map column throws an NPE [M].

## 5. A false NOT NULL flag gives wrong results [M]

`createDataFrame` turns a null array element into 0; the Delta table above turns `[1,null]` into
`[1,1]`; `Narrow(bad0).filter(id isNull)` returns nothing. The file and JSON readers reset the flags,
so they cannot be made to lie (out-q5).

## Verdict

Exact nullability including narrowing is buildable with the public API, batch and streaming [M], as
the final projection of every `io` load. The error surfaces at the action, with a message Pathling
writes. Open points for the spec:

1. Whether the check covers only materialised data (enough for export), or a guard filter is added
   to cover `count()` and narrower selects. Neither covers a downstream `filter(isNull)` on a
   `NOT NULL`-marked output.
2. Delta sinks should create tables with the nullable form of the schema, because Delta neither
   checks array elements or map values nor accepts a null optional parent with a NOT NULL child.
