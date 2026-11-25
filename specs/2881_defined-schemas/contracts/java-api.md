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

`withSchemas` adds a schema-set component to the `DataSourceBuilder` record. This is a public API
change: the canonical constructor gains a parameter (an empty set means no schemas), and the record
gains an accessor for it, with `equals`, `hashCode` and `toString` following. The released
`DataSourceBuilder(PathlingContext)` constructor is kept. The unreleased
`DataSourceBuilder(PathlingContext, boolean)` canonical constructor is replaced, not kept.

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
- A streaming `df` with a null schema throws `UnsupportedOperationException`, as today, with the
  message of research R12, which keeps the `foreachBatch` recommendation.

## Writing a stream

No new method. A streaming encode is written to a lake through a Pathling sink inside
`foreachBatch` (research R8), so every sink check applies to each micro-batch.
