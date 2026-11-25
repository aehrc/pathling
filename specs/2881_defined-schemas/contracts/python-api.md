# Python API contract

A schema is a JSON-compatible `dict` (as from `json.loads(struct.json())`) or a JSON string. A
schema set is a `dict[str, schema]` or a JSON string. Spark `StructType` is not accepted (FR-004).

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
pc.read.parquet(path, schemas=None, layout_detection=True)  # Was Dict[str, StructType].
pc.read.delta(path, schemas=None, layout_detection=True)
pc.read.tables(schema=None, schemas=None, layout_detection=True)
pc.read.bulk(..., schemas=None)

# Schemas from data.
DataSource.schemas() -> dict[str, dict]

# Encoding.
pc.encode(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame
pc.encode_bundle(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame

# Writing a stream: no new method. Use a Pathling sink inside foreachBatch (research R8).
stream.writeStream.foreachBatch(
    lambda df, _: pc.read.datasets({"Patient": df}).write.delta(path, save_mode="merge")
).start()
```
