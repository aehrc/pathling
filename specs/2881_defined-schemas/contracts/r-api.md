# R API contract

A schema is a list (as from `jsonlite::fromJSON(json, simplifyVector = FALSE)`) or a JSON
string. A schema set is a named list of schemas, or a JSON string. There is no limit on the number
of entries. The 10-entry `Map.of` cap of `to_java_schema_map` is removed.
A list is converted to JSON with an empty field `metadata` written as `{}`: `jsonlite` would
otherwise write an empty unnamed `list()` as `[]`, which Spark does not accept as metadata.

Argument order matches the Python binding: on `pathling_read_parquet`, `pathling_read_delta` and
`pathling_read_tables`, `schemas` follows the location argument and precedes `layout_detection`,
as `pathling_read_parquet` already has it. On the other readers `schemas` is the last argument.

```r
# Sources: new argument on every reader.
pathling_read_ndjson(pc, path, extension = "ndjson", file_name_mapper = NULL, schemas = NULL)
pathling_read_bundles(pc, path, resource_types, mime_type = MimeType$FHIR_JSON, schemas = NULL)
pathling_read_datasets(pc, resources, schemas = NULL)
pathling_read_parquet(pc, path, schemas = NULL, layout_detection = TRUE)  # Now JSON form.
pathling_read_delta(pc, path, schemas = NULL, layout_detection = TRUE)
pathling_read_tables(pc, schema = NULL, schemas = NULL, layout_detection = TRUE)
pathling_read_bulk(pc, ..., schemas = NULL)

# Schemas from data: a named list of schemas.
ds_schemas(ds)

# Encoding.
pathling_encode(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)
pathling_encode_bundle(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)

# Writing a stream: no new function. The documented route is a Pathling sink inside
# foreachBatch (research R8), which sparklyr does not offer.
```
