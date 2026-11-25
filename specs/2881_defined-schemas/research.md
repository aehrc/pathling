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
  with the same reader options from `FhirJsonReader.json(spark)`. Parity with the inferred load
  wins over keeping the lexical form: if the inferred read gives a double, so does the defined
  read. T003 only confirms which type the inferred read gives, recorded in
  `evidence/decimal-read.md`.
- **Rationale**: FR-009 and FR-021a need the defined load to equal the inferred load of the same
  data with the same schema. The survey could not confirm the reader's behaviour from code alone.
- **Alternatives considered**: Always double. This would lose precision relative to the inferred
  path if that path keeps text.

## R4. Non-bundle JSON documents

- **Decision**: With a defined schema, `FhirJsonReader.read(type, Dataset<String>)` uses
  `from_json(value, observed)` followed by `toLayout(type, observed, col, identity)`, the same
  shape as `readBundles`. File reads (`read(type, path)`) use `spark.read.schema(observed).json`.
  Without a schema, both are unchanged.

    A defined bundle read, JSON or XML, keeps everything `readBundles` does today except the
    inference read that gives `observed`: the splitter UDF (`BundleResource.resources` with
    `SPLIT`), the `explode` of its entries, the parse, and reference resolution. JSON passes
    `reference -> BundleResource.resolve(reference, references)` to `toLayout`, as
    `FhirJsonReader.readBundles` does; XML keeps the references column and the `BundleResource.resolve`
    call in `FhirXmlReader`'s private `toLayout`. So `urn:uuid` references within a bundle resolve
    the same with and without a schema.

- **Rationale**: `spark.read.json(Dataset)` goes through an RDD even when a schema is given, so it
  is not stream-safe. T002 confirms this in `evidence/stream-safety.md`, together with the steps a
  defined bundle read keeps: the bundle splitter UDF followed by `explode` over a streaming
  dataset, and `from_xml` over a streaming dataset.

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
        - never cast. A leaf type clash is left to #2884, which reads it as null.
    2. Annotate with `Annotator.annotate` per the configuration.
    3. Log at debug level the paths of the stored schema that the target lacks, other than
       annotations (FR-006a). This compares two schemas the source already holds, so it runs no
       Spark job and touches no row.

    `Projection` is `DeltaSink.conformToTarget` extracted to `io`. `DeltaSink`'s merge then uses it.

    The hook differs per source, so that each read is shaped exactly once:
    - **Parquet and Delta**: the transformer function that `ParquetSource` and `DeltaSource` pass
      to the `FileSource` constructor, today the identity `(sourceData, resourceType) ->
sourceData`. `FileSource.load` applies it after `LayoutDetector.check`. `FileSource.load`
      itself is not changed, because `NdjsonSource` and `BundlesSource` share it and their
      transformers (`context.encode`, `context.encodeBundle`) already shape the data with the
      defined-schema reader. `ParquetSource.readerFor` stops binding the schema with `.schema(...)`.
    - **Catalog tables**: `CatalogSource.read`, after `LayoutDetector.check` and before
      `universalOperator`.
    - **Datasets**: `DatasetSource.dataset(type, df)`, where a dataset is registered. Not
      `DatasetSource.read`, which `FileSource` inherits, so projecting there would shape NDJSON
      and bundle reads a second time.

- **Rationale**: Every path lands on the same schema (FR-009). The projector already exists in
  Column-API form, so `io` may hold it. Recomputing annotations satisfies FR-007 and covers a
  stored table annotated under another configuration. The dropped paths are known from the schemas
  alone for stored data, so logging them is free. NDJSON, bundle and XML reads with a defined
  schema run no inference, so the content they drop could be found only by per-row work; they do
  not look for it.
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
    - **Python**: a `schemas=` keyword on each `read.*` method, a `schema=` keyword on
      `encode` and `encode_bundle`, and `DataSource.schemas()` returning a dict of dicts.
    - **R**: a `schemas` argument on each `pathling_read_*` function, a `schema`
      argument on `pathling_encode*`, and `ds_schemas()`.

    Bindings accept the JSON form only (FR-004). Python takes a dict or a JSON string. R takes a list
    or a JSON string, converted with `jsonlite::toJSON(auto_unbox = TRUE)`.

- **Rationale**: Builder methods are the Java idiom (`withLayoutDetection`). Keywords are the Python
  and R idiom. Strings cross Py4J and sparklyr without a type conversion, and that removes R's
  10-entry `Map.of` cap (`to_java_schema_map`).
- **Alternatives considered**: Spark `StructType` arguments. Rejected by FR-004, because they do not
  round-trip through a file the user keeps.

## R8. Writing a stream to a lake

- **Decision**: There is no destination check. A stream of encoded resources is written to a lake
  with `foreachBatch` and a Pathling sink, extending the 10.x `foreachBatch` idiom:

    ```java
    patients.writeStream()
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, id) ->
                    pc.read().datasets().dataset("Patient", batch).write()
                        .saveMode("merge").delta(path))
        .start();
    ```

    ```python
    patients.writeStream.foreachBatch(
        lambda df, _: pc.read.datasets({"Patient": df}).write.delta(path, save_mode="merge")
    ).start()
    ```

- **Rationale**: The sink runs its layout and timestamp refusals on every micro-batch, so nothing
  a sink checks is skipped, and no new public method is needed.
- **Alternatives considered**: A public `checkDestination` for a raw `writeStream`, in an earlier
  draft (the old FR-015 and User Story 3). Removed: `foreachBatch` with a Pathling sink keeps every
  check.

## R9. Reading back streamed output

- **Decision**: No new code is expected.
    - Delta: a streaming write to `<path>/<name>.parquet` is a Delta table that `DeltaSource` already
      reads.
    - Parquet: `FileSource.retrieveFilesFromPath` maps the directory `<name>.parquet` to its type, and
      Spark reads a streaming sink directory through its `_spark_metadata` log.

    FR-014 and its test T030 were later removed: a stream is written through a Pathling sink in
    `foreachBatch`, so this read-back is not promised.

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
       missing schema elements and batches carrying extra elements. "Equals" is the schema
       equality of spec SC-002, implemented once per binding as a test helper
       (`SchemaEquality` in Java, `schema_equality.py` in Python, `helper_schema_equality.R` in
       R).
    2. **Path parity** (FR-009, SC-004): JSON = XML = stored Parquet.
    3. **Absence** (FR-021, SC-005): extend `AbsentElementTest` and its peers to any position kind
       not yet covered. The survey found:
        - existing coverage of top-level, nested, choice, metadata-group, annotation, recursion and
          one-sided combinations;
        - gaps in a repeating complex element absent on both sides of a combination, and an absent
          primitive metadata group inside a repeating complex element.
    4. **YAML suites on defined reads** (FR-021b), in the regular build, for both
       `YamlReferenceImplTest` (fhirpath.js) and `YamlFhirPathTest` (ptl). Today a case loads
       one of two ways (`YamlTestArgumentProvider`, lines 137–197):
        - **Path 1**: a subject that is a FHIR resource (an `inputfile` from the suite's resource
          directory, such as the 19 files of `fhirpath/src/test/resources/fhirpath-js/resources/`,
          or an inline subject with a valid `resourceType`) goes through `FhirResolverFactory` to
          `PofDatasets.fromJson`, and so to `FhirReader.json().read(type, Dataset<String>)` with
          inference.
        - **Path 2**: any other subject goes through `ArbitraryObjectResolverFactory`, which builds
          a definition from the YAML (`YamlSupport.yamlToDefinition`) and a hand-built stored
          schema (`YamlSupport.definitionToStruct(def, subject)`, with `storedType` and
          `storedCodingType`), and reads with `spark.read().schema(...).json(...)`, bypassing
          `FhirReader`.

        The new design changes both:
        - **Path 2** reads through
          `FhirReader.of(spark, DefaultDefinitionContext.of(subjectDefinition), config).json()
.read(code, docs, definitionToStruct(subjectDefinition))`: a defined read with the schema
          derived from the definition alone. `FhirReader` accepts any `DefinitionContext`. The
          `YamlSupport` helpers that served only the hand-built stored layout (`storedType`,
          `storedCodingType`, the subject-pruned `definitionToStruct` overload, and `subjectToJson`
          if no longer needed) are removed once a check of their usages, including
          `YamlTestRunnerTest`, shows nothing else needs them.
        - **Path 1** loads each case with a defined schema: the schema inferred from all of the
          suite's input resources of the case's type, merged by name with the case's own inferred
          schema, so it is always at least as wide as the data. It is derived at test time and
          cached per suite and type. Nothing is checked in. The inferred load stays covered by the
          `io` and `library-api` tests.

        A case whose result changes is an engine or reader defect to fix or, if it is a genuine
        conformance gap, an entry in the exclusion baselines (the `pathling-yaml-exclusions`
        skill), where it is still executed and asserted to fail in the recorded way. These schemas
        are only ever wider than the data, so narrowing (FR-006) is covered by the load tests.
        `FhirViewShareableComplianceTest` is not part of this. The `dense` placeholder in
        `TestSchemaMode` is superseded and removed.

- **Rationale**: The defined read replaces the inferred one in the YAML suites rather than running
  beside it, so the regular build grows only by the schema derivation (SC-005), and every YAML case
  exercises the defined-schema read. A difference between two schemas holding the same content is
  an engine defect, which the absence tests also target directly.
- **Alternatives considered**:
    - Running every suite per schema source in the regular build. This multiplies the slowest
      suites, and ADR-0007 rejects it.
    - An opt-in profile with a checked-in widening schema set inferred from the FHIR specification
      examples, in an earlier draft. Rejected: it runs outside the regular build, so a regression
      surfaces late, and the checked-in set has to be maintained.

## R12. Streaming refusal message

- **Decision**: `PathlingContext.requireEncodable` refuses a streaming dataset only when no schema
  is given. It keeps today's `UnsupportedOperationException` and its `foreachBatch`
  recommendation. The new message is: "Encoding a streaming dataset needs a schema: resources are
  otherwise stored with a schema inferred from the data, which a stream cannot provide. Pass a
  schema, for example one taken from an existing table's schemas(), or encode each batch within
  foreachBatch instead."

## R13. No schema option on sinks

- **Decision**: Sinks take no schema set. A sink writes what its source produced.
- **Rationale**: Every source takes a schema set, so data reaching a sink was either loaded with
  one and already has its schema, or was loaded without one on purpose. A sink option would only
  duplicate the source option. Reading with one set and writing with another is schema migration,
  which is out of scope. On the NDJSON sink it would drop content from the output.
- **Alternatives considered**: A schema set on every sink, in the first draft (the old FR-011).
  Removed for the reasons above.
