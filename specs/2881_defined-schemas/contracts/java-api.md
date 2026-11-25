# Java API contract

All additions are public API of `library-api`. Every call that is given no schema behaves as it
does today (FR-020).

## Sources: `DataSourceBuilder`

```java
// Returns a builder whose every source method projects the named types and infers the rest.
@Nonnull DataSourceBuilder withSchemas(@Nonnull String schemaSetJson);
@Nonnull DataSourceBuilder withSchemas(@Nonnull Map<String, String> schemaSet);

// Removed (unreleased): parquetWithSchemas(String, Map<String, StructType>).
```

The set applies to `ndjson`, `bundles`, `datasets`, `parquet`, `delta`, `tables` and `bulk`.
Errors are `IllegalArgumentException` naming the offending key: an unknown resource type, or a
malformed or non-struct schema.

## Schemas from data: `QueryableDataSource`

```java
// Resource type code → schema JSON, one entry per resource type held.
@Nonnull Map<String, String> schemas();
```

For a Parquet source whose files differ, each schema is the merged schema the reader returns.

## Sinks

Unchanged. A sink writes what its source produced, so a source given a schema set writes
that set's schemas.

## Encoding: `PathlingContext`

```java
@Nonnull Dataset<Row> encode(@Nonnull Dataset<?> df, @Nonnull String resourceType,
    @Nonnull String inputMimeType, @Nullable String schemaJson);
@Nonnull Dataset<Row> encodeBundle(@Nonnull Dataset<?> df, @Nonnull String resourceType,
    @Nonnull String inputMimeType, @Nullable String schemaJson);
```

- A streaming `df` with a non-null schema returns a streaming dataset.
- A streaming `df` with a null schema throws `IllegalArgumentException` with the message of
  research R12.

## Destination check: `PathlingContext`

```java
// Returns resource type → table path. Throws the sinks' own exceptions:
// UnsupportedLayoutException for a legacy-layout table, and IllegalStateException
// for the timestamp setting.
@Nonnull Map<String, String> checkDestination(@Nonnull String format, @Nonnull String path,
    @Nonnull Map<String, Dataset<Row>> datasets);
```

`format` is `delta` or `parquet`. Datasets may be streaming, because only their schemas are read.
