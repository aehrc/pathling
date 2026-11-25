# Research: Load FHIR resources with a schema defined up front

## R1. How a defined schema drives a load

- **Decision**: The defined schema is converted into the observed shape that
  `ResourceTransformer.toLayout(type, observed, column, references)` takes, and the input is
  parsed with that shape.
    - JSON uses `from_json(document, observed)`.
    - XML uses `from_xml(document, parseSchema)`.

    The walk then types, orders and prunes the result from the definitions, as it does today
    (ADR-0008).

- **Rationale**: This is one code path for inferred and defined loads. Only where `observed` comes
  from changes. The output is a PoF schema by construction (FR-008). There is no action on the
  input, so a stream works (FR-012).
- **Alternatives considered**:
    - Infer, then project: needs a pass over the data, so it cannot serve streams.
    - Cast to the defined types: ADR-0005 reserves types for the definitions.

## R2. Deriving the observed shape from a PoF schema

- **Decision**: Add a `ResourceTransformer` method that walks the defined schema against the
  canonical structure and returns the JSON-model shape. It follows the walk of the egress
  (`egressFields`), with two differences:
    1. A field the definitions do not describe is skipped, not thrown on. Today `storedEntry` throws
       `IllegalStateException` (ResourceTransformer:754-760).
    2. Annotation fields are skipped (FR-007).

    Leaf types follow what `toLayout` ingests with `try_cast`:
    - integers become int or long;
    - booleans stay boolean;
    - text, dates and codes become string;
    - `base64Binary` becomes string;
    - decimal becomes the type the inferred JSON read gives it (see R3).

    Metadata groups `_name` become `{id, extension}` structs where the definitions allow them
    (`MetadataGroups.holdsMetadata`). Repeating elements become arrays, and complex elements become
    structs.

- **Rationale**: The egress walk already gives this mapping for canonical input (the survey
  confirmed `toJsonShape` on an empty frame gives a usable schema with no job). Making it skip
  rather than throw covers the non-PoF case: "treated as inference treats an observed shape".
- **Alternatives considered**:
    - Calling `toJsonShape` unchanged: it throws on a non-canonical top-level field, and it marks
      decimals.
    - Using the PoF schema directly as the JSON read schema: the layouts differ (decimal text,
      binary, annotations), so values would be nulled.

## R3. Decimal read type

- **Decision**: The derived shape reads a decimal exactly as the inferred JSON read does today,
  with the same reader options from `FhirJsonReader.json(spark)`. This is confirmed by T003, which
  is recorded in `evidence/decimal-read.md`. If Spark returns the raw number text for a
  string-typed field, string is used, because it keeps the lexical form. If it does not, the
  inferred type is used.
- **Rationale**: FR-009 and FR-021a need the defined load to equal the inferred load of the same
  data with the same schema. The survey could not confirm the reader's behaviour from code alone.
- **Alternatives considered**: Always double. This would lose precision relative to the inferred
  path if that path keeps text.

## R4. Non-bundle JSON documents

- **Decision**: With a defined schema, `FhirJsonReader.read(type, Dataset<String>)` uses
  `from_json(value, observed)` followed by `toLayout(type, observed, col, identity)`, the same
  shape as `readBundles`. File reads (`read(type, path)`) use `spark.read.schema(observed).json`.
  Without a schema, both are unchanged.
- **Rationale**: `spark.read.json(Dataset)` goes through an RDD even when a schema is given, so it
  is not stream-safe. T002 confirms this in `evidence/stream-safety.md`.

## R5. XML

- **Decision**: With a defined schema, skip the inference read at FhirXmlReader:181-188 and build
  the parse schema with `XmlIngestShape` from the derived observed shape, making every repeating
  element an array. `wrap` (:384-395) already accepts both array and single parsed types. The rest
  is unchanged: `from_xml`, then `shape()`, then `toLayout`.
- **Rationale**: Only the inference call blocks streaming. The parse step is pure Column work.

## R6. Projecting stored data (Parquet, Delta, tables, datasets)

- **Decision**: The target is the layout schema, which is the output schema of
  `toLayout(type, observed, …)` for the defined schema, computed over an empty frame. Stored data
  is projected as follows:
    1. Project by name to the target with `Projection.project(Dataset, StructType)`:
        - drop fields the target lacks, including stored annotations;
        - add missing fields as typed nulls;
        - recurse through structs, arrays and maps;
        - keep a null struct null;
        - never cast.
    2. Annotate with `Annotator.annotate` per the configuration.

    `Projection` is `DeltaSink.conformToTarget` extracted to `io`. `DeltaSink`'s merge then uses it.
    The hook is `FileSource.load` after `LayoutDetector.check`, `CatalogSource.read`, and
    `DatasetSource.read`.

- **Rationale**: Every path lands on the same schema (FR-009). The projector already exists in
  Column-API form, so `io` may hold it. Recomputing annotations satisfies FR-007 and covers a
  stored table annotated under another configuration.
- **Alternatives considered**: `MergeCast.project` uses the Catalyst API and lives in `encoders`.
  `spark.read.schema()` on Parquet (the current `parquetWithSchemas`) cannot reorder nested
  structs or drop nested fields reliably, and does not apply to Delta tables or datasets.

## R7. API shape

- **Decision**:
    - **Java**:
        - `DataSourceBuilder.withSchemas(String)` and `withSchemas(Map<String, String>)` return a
          builder whose every source method applies the set.
        - `encode` and `encodeBundle` overloads with a trailing `@Nullable String schema`.
        - `QueryableDataSource.schemas()` returns `Map<String, String>`.
        - `PathlingContext.checkDestination(format, path, datasets)`.
    - **Python**: a `schemas=` keyword on each `read.*` method, a `schema=` keyword on
      `encode` and `encode_bundle`, `DataSource.schemas()` returning a dict of dicts, and
      `pc.check_destination(...)`.
    - **R**: a `schemas` argument on each `pathling_read_*` function, a `schema`
      argument on `pathling_encode*`, `ds_schemas()`, and `pathling_check_destination()`.

    Bindings accept the JSON form only (FR-004). Python takes a dict or a JSON string. R takes a list
    or a JSON string, converted with `jsonlite::toJSON(auto_unbox = TRUE)`.

- **Rationale**: Builder methods are the Java idiom (`withLayoutDetection`). Keywords are the Python
  and R idiom. Strings cross Py4J and sparklyr without a type conversion, and that removes R's
  10-entry `Map.of` cap (`to_java_schema_map`).
- **Alternatives considered**: Spark `StructType` arguments. Rejected by FR-004, because they do not
  round-trip through a file the user keeps.

## R8. Destination check

- **Decision**: `checkDestination(String format, String path, Map<String, Dataset<Row>> datasets)`
  for `delta` and `parquet` returns `Map<String, String>` of resource type → table path
  (`<path>/<fileNameMapper(type)>.parquet`). For each type it runs:
    1. `LayoutDetector.check` on an existing table at that path, giving the legacy-layout refusal;
    2. `StoredTimestamps.requireMicros` on the datasets' schemas, giving the timestamp refusal.

    The datasets may be streaming, because only their schemas are read. The sinks call the same
    `DestinationCheck`, so the errors are identical (FR-015).

- **Rationale**: The timestamp refusal depends on whether the data carries range annotations,
  which only the encoded datasets' schemas tell. Taking the datasets rather than bare type names
  makes the check exactly the sink's. Returning the paths gives the streaming writer the location
  that FR-014 needs.
- **Alternatives considered**: Bare resource types plus a guess from the configuration. This could
  disagree with the sink.
- **Spec reconciliation**: FR-015 first said "a set of resource types". The datasets' keys are that
  set. FR-015 and US3 scenario 2 now say "datasets", and FR-015 says the check returns the paths.

## R9. Reading back streamed output

- **Decision**: No new code is expected.
    - Delta: a streaming write to `<path>/<name>.parquet` is a Delta table that `DeltaSource` already
      reads.
    - Parquet: `FileSource.retrieveFilesFromPath` maps the directory `<name>.parquet` to its type, and
      Spark reads a streaming sink directory through its `_spark_metadata` log.

    Test T028 proves this, and an implementation task follows only if it fails.

## R10. Schema set parsing and errors

- **Decision**: `io` `SchemaSet.parse(String json)` and `SchemaSet.of(Map<String, String>)` parse
  each entry with `DataType.fromJson`. The parse refuses, with an error naming the key:
    - a key that is not a resource type code known to the definitions;
    - a value that is not a struct;
    - a value that is malformed.

    An empty set equals no set.

- **Rationale**: These are the spec's edge cases. One parser serves every entry point.

## R11. Testing strategy (ADR-0007)

- **Decision**:
    1. **Load tests** (FR-021a, SC-003): for every source and encode path, the output schema
       equals the given PoF base schema plus the configured annotations, including for batches
       missing schema elements and batches carrying extra elements.
    2. **Path parity** (FR-009, SC-004): JSON = XML = stored Parquet.
    3. **Absence** (FR-021, SC-005): extend `AbsentElementTest` and its peers to any position kind
       not yet covered. The survey found:
        - existing coverage of top-level, nested, choice, metadata-group, annotation, recursion and
          one-sided combinations;
        - gaps in a repeating complex element absent on both sides of a combination, and an absent
          primitive metadata group inside a repeating complex element.
    4. **Parity profile** (FR-021b): `-PdefinedSchemaParity` sets `pathling.testSchemaMode=defined`.
       `PofDatasets.fromJson` then loads each case with a defined schema, taken from a checked-in
       widening schema set (`fhirpath/src/test/resources/defined-schemas/widening.json`, inferred
       from the FHIR specification examples) merged by name with the case's inferred schema. The
       result is therefore always wider than the data, and the defined load reorders it to a PoF
       schema. The profile runs the YAML FHIRPath suites and `FhirViewShareableComplianceTest`.
       Release workflows do not run it.
- **Rationale**: This keeps the regular build to the new tests (SC-005). A difference between two
  schemas holding the same content is an engine defect, which the absence tests target directly.
- **Alternatives considered**: Running every suite per schema source in the regular build. This
  multiplies the slowest suites, and ADR-0007 rejects it.

## R12. Streaming refusal message

- **Decision**: `PathlingContext.requireEncodable` refuses a streaming dataset only when no schema
  is given. The new message is: "Encoding a streaming dataset needs a schema: resources are
  otherwise stored with a schema inferred from the data, which a stream cannot provide. Pass a
  schema, for example one taken from an existing table's schemas()."

## R13. No schema option on sinks

- **Decision**: Sinks take no schema set. A sink writes what its source produced.
- **Rationale**: Every source takes a schema set, so data reaching a sink was either loaded with
  one and already has its schema, or was loaded without one on purpose. A sink option would only
  duplicate the source option. Reading with one set and writing with another is schema migration,
  which is out of scope. On the NDJSON sink it would drop content from the output.
- **Alternatives considered**: A schema set on every sink, in the first draft (the old FR-011).
  Removed for the reasons above.
