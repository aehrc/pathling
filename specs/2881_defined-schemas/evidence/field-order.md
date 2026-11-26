# Whether Parquet, Delta and catalog tables accept batches that differ only in field order

Measured on 2026-11-26 with PySpark 4.0.2 and Delta 4.0.1, using
[field-order-probe.py](field-order-probe.py), for the decision that field order does not count
towards whether a defined schema is acceptable. Two schemas are **equal** when they have the same
fields, names and types in the same order at every depth, and **equivalent** when their canonical
forms are equal: equal up to field order.

Batch A has its fields in canonical order. Batch B has the same fields reordered at the top level,
inside a struct and inside an array of structs. The swapped fields share a type, so a write or read
that matched fields by position would swap values silently rather than fail.

**Result: every write accepts equivalent batches and matches fields by name at every depth. A
Delta table's order is its first writer's, a Parquet directory's is that of whichever file Spark
reads first, and fields added later go last.**

| Write onto a table first written with A                         | Accepted | Values correct |
| --------------------------------------------------------------- | -------- | -------------- |
| Parquet append, read with or without `mergeSchema`              | yes      | yes            |
| Delta append, without `mergeSchema`                             | yes      | yes            |
| Delta append, with `mergeSchema`                                | yes      | yes            |
| Delta `MERGE … UPDATE SET * / INSERT *`, inserting and updating | yes      | yes            |
| Catalog Parquet table, `saveAsTable` append, as `CatalogSink`   | yes      | yes            |

The order a table reports is never changed by accepting a batch:

- A Delta table keeps the order of the batch that created it. Created from B and appended with A,
  it reports B's order.
- A Parquet directory takes its order from the footer of the file Spark reads first, with or
  without `mergeSchema`; with it, fields from other files go last. Part-file names carry random
  identifiers, so that file is not necessarily the first written: across runs of this probe the
  same directory reported A's order in some runs and B's in others, whichever was written first.
  A catalog Parquet table read through the catalog keeps the order it was created with.
- Delta schema evolution puts a new field last, even inside a struct:
  `coding: struct<system, code, version>`, where the definitions put `version` between them.

## Reordering with `Dataset.to`

`Dataset.to(schema)` reorders a canonical batch into a target order by name, at the top level and
inside structs, arrays of structs and maps of structs, with the values in their right fields and a
schema equal to the target. It is the step a user would need after every load if a defined read
always produced canonical order; research R14 rejects that design in favour of keeping the defined
order and canonicalising schemas on request.

## Consequences

- Writing to a lake needs no canonical order from the batch. Pathling's `DeltaSink` projects by
  name onto the target in any case.
- Storage does not preserve canonical order. Once another writer or schema evolution touches a
  table, its order may differ, so a schema returned by `schemas()` is equivalent to the canonical
  one but not always equal to it (FR-019).
- Order matters only to the operations that match by position: struct cast, struct `=`, `union`
  and `INSERT INTO … SELECT`. Nothing in Pathling uses them on resource data, so order never
  affects correctness inside Pathling.
- A defined schema is therefore accepted in any order equivalent to a valid one: refusing
  non-canonical order would refuse the schemas that evolved tables report. A read with it keeps
  its order, so that its output lines up with the table it came from (research R14).

## Output

One run. The order reported for the Parquet directories varies between runs, as described above.

```text
A schema: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
B schema: struct<coding:struct<code:string,system:string>,name:array<struct<given:string,family:string>>,id:string>
--- parquet append, read: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- parquet append, read with mergeSchema: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta append: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta append with mergeSchema: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta merge, insert: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta merge, update: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sB', 'cB', 'fB', 'gB')
--- parquet B first, read: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- parquet B first, read with mergeSchema: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta B first, A appended: struct<coding:struct<code:string,system:string>,name:array<struct<given:string,family:string>>,id:string>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- delta evolved: struct<id:string,coding:struct<system:string,code:string,version:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    3 ('s3', 'c3', 'f3', 'g3')
--- catalog parquet table, B appended: struct<id:string,coding:struct<system:string,code:string>,name:array<struct<family:string,given:string>>>
    1 ('sA', 'cA', 'fA', 'gA')
    2 ('sB', 'cB', 'fB', 'gB')
--- Dataset.to: struct<name:array<struct<given:string,family:string>>,m:map<string,struct<code:string,system:string>>,coding:struct<code:string,system:string>,id:string> equal to target: True
    s c f g ms mc
```
