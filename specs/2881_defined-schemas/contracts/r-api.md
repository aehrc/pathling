# R API contract

A schema is a list (as from `jsonlite::fromJSON(json, simplifyVector = FALSE)`) or a JSON
string. A schema set is a named list of schemas, or a JSON string. There is no limit on the number
of entries. The 10-entry `Map.of` cap of `to_java_schema_map` is removed.

```r
# Sources: new argument on every reader.
pathling_read_ndjson(pc, path, extension = "ndjson", file_name_mapper = NULL, schemas = NULL)
pathling_read_bundles(pc, path, resource_types, mime_type = MimeType$FHIR_JSON, schemas = NULL)
pathling_read_datasets(pc, resources, schemas = NULL)
pathling_read_parquet(pc, path, schemas = NULL, layout_detection = TRUE)  # Now JSON form.
pathling_read_delta(pc, path, layout_detection = TRUE, schemas = NULL)
pathling_read_tables(pc, schema = NULL, layout_detection = TRUE, schemas = NULL)
pathling_read_bulk(pc, ..., schemas = NULL)

# Schemas from data: a named list of schemas.
ds_schemas(ds)

# Encoding.
pathling_encode(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)
pathling_encode_bundle(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)

# Destination check: a named list of resource type -> table path.
pathling_check_destination(pc, format, path, datasets)
```
