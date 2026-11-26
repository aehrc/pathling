# R API contract

sparklyr has no `StructType`, so a schema is a string holding a struct type's JSON form, as Spark
writes it, and a schema set is a named list of such strings, keyed by resource type code (FR-004).
The strings are opaque: the user takes them from `ds_schemas` or `pathling_read_schemas` and passes
them on, and they cross to the JVM unchanged. A string that is not a struct type's JSON form is
refused with an error naming its key. A schema that is not a PoF schema fails the read,
`pathling_encode` or `pathling_encode_bundle` with an error naming the resource type and path
(FR-006b). There is no limit on the number of entries. The 10-entry
`Map.of` cap of `to_java_schema_map` is removed.

Argument order matches the Python binding: on `pathling_read_parquet`, `pathling_read_delta` and
`pathling_read_tables`, `schemas` follows the location argument and precedes `layout_detection`,
as `pathling_read_parquet` already has it. On the other readers `schemas` is the last argument.

```r
# Sources: new argument on every reader.
pathling_read_ndjson(pc, path, extension = "ndjson", file_name_mapper = NULL, schemas = NULL)
pathling_read_bundles(pc, path, resource_types, mime_type = MimeType$FHIR_JSON, schemas = NULL)
pathling_read_datasets(pc, resources, schemas = NULL)
pathling_read_parquet(pc, path, schemas = NULL, layout_detection = TRUE)  # Strings only.
pathling_read_delta(pc, path, schemas = NULL, layout_detection = TRUE)
pathling_read_tables(pc, schema = NULL, schemas = NULL, layout_detection = TRUE)
pathling_read_bulk(pc, ..., schemas = NULL)

# Schemas from data: a named list of schemas.
ds_schemas(ds)

# Canonicalisation (FR-027): the same set in canonical field order, as a named list of strings.
# Reads no data.
pathling_canonical_schemas(pc, schemas)

# Schema set files (FR-028, data-model.md). pathling_read_schemas returns a named list of strings,
# and refuses a malformed file with an error naming it, and a value that is not a struct with an
# error naming its key.
pathling_write_schemas(schemas, path)
pathling_read_schemas(path)

# Encoding.
# schema is a string holding a struct type's JSON form.
pathling_encode(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)
pathling_encode_bundle(pc, df, resource_name, input_type = NULL, column = NULL, schema = NULL)

# Writing a stream: no new function. The documented route is a Pathling sink inside
# foreachBatch (research R8), which sparklyr does not offer.
```
