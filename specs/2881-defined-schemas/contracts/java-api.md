# Java API contract

All additions are public API of `library-api`. Every call that is given no schema behaves as it
does today (FR-020). A schema is a `StructType`, and a schema set is a `Map<String, StructType>`
from resource type code to schema (FR-004). No class wraps the set.

## Sources: `DataSourceBuilder`

```java
// Returns a builder whose every source method projects the named types and infers the rest.
@Nonnull DataSourceBuilder withSchemas(@Nonnull Map<String, StructType> schemaSet);

// Removed (unreleased): parquetWithSchemas(String, Map<String, StructType>).
```

`withSchemas` adds a schema-set component to the `DataSourceBuilder` record. This is a public API
change: the canonical constructor gains a parameter (an empty set means no schemas), and the record
gains an accessor for it, with `equals`, `hashCode` and `toString` following. The released
`DataSourceBuilder(PathlingContext)` constructor is kept. The unreleased
`DataSourceBuilder(PathlingContext, boolean)` canonical constructor is replaced, not kept.

The set applies to `ndjson`, `bundles`, `datasets`, `parquet`, `delta`, `tables` and `bulk`.
Errors are `IllegalArgumentException` naming the offending key: an unknown resource type, or a
null schema. A schema that is not a PoF schema fails the read, `encode` or `encodeBundle` with the
error of the check of a schema against the uber-schema that #2884 adds, naming the resource type
and path (FR-006b).

## Schemas from data: `QueryableDataSource`

```java
// Resource type code → schema, one entry per resource type held.
@Nonnull Map<String, StructType> schemas();
```

For a Parquet source whose files differ, each schema is the merged schema the reader returns.

`schemas()` is a default method, which reads the schema of each type `getResourceTypes()` names, in
order (research R21).

## Canonicalisation: `PathlingContext`

```java
// The same set with every schema's fields in canonical field order (FR-027). Reads no data.
@Nonnull Map<String, StructType> canonicalSchemas(@Nonnull Map<String, StructType> schemaSet);
```

## Schema set files: `SchemaSets`

A small public utility class with static methods only, which convert a schema set to and from the
content of a schema set file (data-model.md, FR-028). The caller reads and writes the file.

```java
// The schema set file's content: one JSON object, resource type code → StructType.json() object.
@Nonnull static String toJson(@Nonnull Map<String, StructType> schemaSet);

// Parses that content. Refuses malformed content, and a value that is not a struct with an error
// naming its key, with IllegalArgumentException. Resource type codes are checked by the source
// or canonicalisation the set is given to.
@Nonnull static Map<String, StructType> fromJson(@Nonnull String json);
```

## Sinks

Unchanged. A sink writes what its source produced, so a source given a schema set writes
that set's schemas.

## Encoding: `PathlingContext`

```java
@Nonnull Dataset<Row> encodeWithSchema(@Nonnull Dataset<?> df, @Nonnull String resourceType,
    @Nonnull String inputMimeType, @Nullable StructType schema);
@Nonnull Dataset<Row> encodeBundleWithSchema(@Nonnull Dataset<?> df, @Nonnull String resourceType,
    @Nonnull String inputMimeType, @Nullable StructType schema);
```

- They have names of their own, not overloads of `encode` and `encodeBundle`, whose fourth
  argument is a `@Nullable String` column name, so that a literal null is not ambiguous (research
  R22).
- A null schema fits the schema to the data, as `encode` and `encodeBundle` do.

- A streaming `df` with a non-null schema returns a streaming dataset.
- A streaming `df` with a null schema throws `UnsupportedOperationException`, as today, with the
  message of research R12, which keeps the `foreachBatch` recommendation.

## Writing a stream

No new method. A streaming encode is written to a lake through a Pathling sink inside
`foreachBatch` (research R8), so every sink check applies to each micro-batch.
