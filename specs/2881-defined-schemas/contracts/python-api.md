# Python API contract

A schema is a `pyspark.sql.types.StructType`. A schema set is a `dict[str, StructType]` from
resource type code to schema (FR-004). No other form is accepted: a value that is not a
`StructType` is refused with an error naming its key. A schema that is not a PoF schema fails the
read, `encode` or `encode_bundle` with an error naming the resource type and path, by the check of
a schema against the uber-schema that #2884 adds (FR-006b).

`tables` takes both `schema`, the catalog database to read from, and `schemas`, the schema set.
Its docstring states the difference, so that neither is passed for the other.

Argument order: on the three stored-layout readers (`parquet`, `delta` and `tables`), `schemas`
follows the location argument and precedes `layout_detection`, the order `parquet` already has.
`delta` and `tables` gain `schemas` in that same position. Neither `schemas` nor
`layout_detection` has been released (both are newer than v10.0.0), and every existing call site
passes them by keyword, so no released positional call changes. On the other readers `schemas` is
the last argument.

```python
# Sources (pathling.datasource.DataSources): new keyword on every reader.
pc.read.ndjson(path, ..., schemas=None)
pc.read.bundles(path, resource_types, ..., schemas=None)
pc.read.datasets(resources, schemas=None)
pc.read.parquet(path, schemas=None, layout_detection=True)
pc.read.delta(path, schemas=None, layout_detection=True)
pc.read.tables(schema=None, schemas=None, layout_detection=True)
pc.read.bulk(..., schemas=None)

# Schemas from data.
DataSource.schemas() -> dict[str, StructType]

# Canonicalisation (FR-027): the same set in canonical field order. Reads no data.
pc.canonical_schemas(schemas) -> dict[str, StructType]

# Schema set files (FR-028, data-model.md). Pure Python, over StructType.json() and
# StructType.fromJson; no JVM. read_schemas refuses a malformed file with an error naming it, and
# a value that is not a struct with an error naming its key.
from pathling import read_schemas, write_schemas
write_schemas(schemas, path) -> None
read_schemas(path) -> dict[str, StructType]

# Encoding.
# schema is a StructType.
pc.encode(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame
pc.encode_bundle(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame

# Writing a stream: no new method. Use a Pathling sink inside foreachBatch (research R8).
stream.writeStream.foreachBatch(
    lambda df, _: pc.read.datasets({"Patient": df}).write.delta(path, save_mode="merge")
).start()
```
