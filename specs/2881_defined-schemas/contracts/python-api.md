# Python API contract

A schema is a JSON-compatible `dict` (as from `json.loads(struct.json())`) or a JSON string. A
schema set is a `dict[str, schema]` or a JSON string. Spark `StructType` is not accepted (FR-004).

```python
# Sources (pathling.datasource.DataSources): new keyword on every reader.
pc.read.ndjson(path, ..., schemas=None)
pc.read.bundles(path, resource_types, ..., schemas=None)
pc.read.datasets(resources, schemas=None)
pc.read.parquet(path, schemas=None, layout_detection=True)  # Was Dict[str, StructType].
pc.read.delta(path, layout_detection=True, schemas=None)
pc.read.tables(schema=None, layout_detection=True, schemas=None)
pc.read.bulk(..., schemas=None)

# Schemas from data.
DataSource.schemas() -> dict[str, dict]

# Encoding.
pc.encode(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame
pc.encode_bundle(df, resource_name, input_type=None, column=None, schema=None) -> DataFrame

# Destination check: returns resource type -> table path.
pc.check_destination(format: str, path: str, datasets: dict[str, DataFrame]) -> dict[str, str]
```
