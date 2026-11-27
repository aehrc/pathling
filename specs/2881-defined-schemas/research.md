# Research: Load FHIR resources with a schema defined up front

## R1. How a defined schema drives a load

- **Decision**: The defined schema is converted into the observed shape that
  `ResourceTransformer.toLayout(type, observed, column, references)` takes, and the input is
  parsed with that shape.
    - JSON uses `from_json(document, observed)`.
    - XML uses `from_xml(document, parseSchema)`.

    The walk then types and prunes the result from the definitions, as it does today, and the
    result is reordered into the defined schema's field order (ADR-0008, R14).

- **Rationale**: This is one code path for inferred and defined loads. Only where `observed` comes
  from changes. The defined schema is first checked against the uber-schema with the check
  that #2884 adds, once it is re-scoped, and refused if it is not a PoF schema (FR-006b), so the output equals it (FR-008). There is no
  action on the input, so a stream works (FR-012).
- **Alternatives considered**:
    - Infer, then project: needs a pass over the data, so it cannot serve streams.
    - Cast to the defined types: ADR-0005 reserves types for the definitions.

## R2. Deriving the observed shape from a PoF schema

- **Decision**: Add a `ResourceTransformer` method that walks the defined schema against the
  canonical structure and returns the JSON-model shape. It follows the walk of the egress
  (`egressFields`), with two differences:
    1. A field the definitions do not describe never reaches the walk: the schema has already
       been refused by the check #2884 adds, with an error naming its path (FR-006b). The walk may
       therefore keep throwing, as `storedEntry` does today with `IllegalStateException`
       (ResourceTransformer:754-760), as a guard that should not be hit.
    2. Annotation fields are skipped (FR-007).

    Leaf types follow what `toLayout` ingests with `try_cast`:
    - integers become int or long;
    - booleans stay boolean;
    - text, dates and codes become string;
    - `base64Binary` becomes string;
    - decimal becomes string (see R3).

    Metadata groups `_name` become `{id, extension}` structs where the definitions allow them
    (`MetadataGroups.holdsMetadata`). Repeating elements become arrays, and complex elements become
    structs.

- **Rationale**: The egress walk already gives this mapping for canonical input (the survey
  confirmed `toJsonShape` on an empty frame gives a usable schema with no job). The check of
  FR-006b runs first, so the walk only ever sees a PoF schema.
- **Alternatives considered**:
    - Calling `toJsonShape` unchanged: it throws on a non-canonical top-level field, and it marks
      decimals.
    - Using the PoF schema directly as the JSON read schema: the layouts differ (decimal text,
      binary, annotations), so values would be nulled.

## R3. Decimal read type

- **Decision** (owner, 2026-11-26, gate B): The derived shape reads a decimal as `string`, with the
  same reader options from `FhirJsonReader.json(spark)`. `from_json` gives a number read as text
  the text Spark makes of it, and `toLayout` stores that text. A decimal's parity with the inferred
  load is equality in value, not in text (FR-009, FR-021a).
- **Measured (T003)**: the inferred read gives a decimal `double` wherever a value of the element
  in the dataset has a fraction or an exponent, and otherwise `bigint`, or `decimal(p,0)` where an
  integer is beyond the range of a long. It stores the text of that number as `string`. The type
  therefore depends on the data, and no fixed type stores the inferred read's text for every
  dataset (`evidence/decimal-read.md`). With `string`, the defined read stores the same text as the
  inferred read for integer-only and fraction-only data. On data mixing the two it keeps an
  integer's digits (`100`) where the inferred read stores `100.0`, and keeps an integer whole where
  the inferred read rounds it through a double. Neither keeps the lexical form: both store `1.1`
  for `1.10`.
- **Rationale**: Since the inferred type depends on the data, textual parity cannot hold for every
  dataset. `string` departs from the inferred read only where that read went through a double, and
  there it is nearer the source.
- **Alternatives considered**:
    - `double`, the earlier decision: the same text as the inferred read wherever the data holds a
      fraction, but `100.0` for `100` in integer-only data, which small fixtures and YAML cases
      often are, and an integer beyond a double's precision rounded.
    - `bigint`: fails the read on any fraction.
    - `decimal(38,18)`: pads every value to 18 places and fails beyond its precision.

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

- **Rationale**: `spark.read.json(Dataset)` fails over a streaming dataset even when a schema is
  given: Spark's check of the query refuses it when it is built, without running a job. T002 confirms this in `evidence/stream-safety.md`, together with the steps a
  defined bundle read keeps: the bundle splitter UDF followed by `explode` over a streaming
  dataset, and `from_xml` over a streaming dataset.

## R5. XML

- **Decision**: With a defined schema, skip the inference read at FhirXmlReader:181-188 and build
  the parse schema with `XmlIngestShape` from the derived observed shape, making every repeating
  element an array. `XmlIngestShape.wrap` (XmlIngestShape.java:384-395) already accepts both array and single parsed types. The rest
  is unchanged: `from_xml`, then `shape()`, then `toLayout`.
- **Rationale**: Only the inference call blocks streaming. The parse step is pure Column work.

## R6. Projecting stored data (Parquet, Delta, tables, datasets)

- **Decision**: The target is the layout schema, which is the output schema of
  `toLayout(type, observed, …)` for the defined schema, computed over an empty frame and reordered
  into the defined schema's field order (R14). Stored data is projected as follows:
    1. Check the stored schema against the uber-schema with the check of a schema against the
       uber-schema that #2884 adds, at the point where a read without a schema makes it
       (FR-006c). A defined schema does not skip this step. A clash, such as `Patient.birthDate`
       stored as `int`, is handled as a read without a schema handles it; this specification
       assumes that it fails the read with an error naming the resource type and path, and
       follows #2884 if its OQ-B decides otherwise.
    2. Project by name to the target with `Projection.project(Dataset, StructType)`:
        - drop fields the target lacks, including stored annotations;
        - add missing fields as typed nulls;
        - recurse through structs, arrays and maps;
        - keep a null struct null;
        - never cast. The target's types are the definitions' types, and if step 1 fails a
          clash no leaf type clashes with them. Whether a stored clash is instead cast is #2884's
          to decide (its OQ-B), and would happen in that check, not in the projection.
    3. Annotate with `Annotator.annotate` per the configuration.
    4. Log at debug level the paths of the stored schema that the target lacks, other than
       annotations (FR-006a). This compares two schemas the source already holds, so it runs no
       Spark job and touches no row.

    `Projection` is `DeltaSink.conformToTarget` extracted to `io`. `DeltaSink`'s merge then uses
    it.

    The hook differs per source, so that each read is shaped exactly once:
    - **Parquet and Delta**: the transformer function that `ParquetSource` and `DeltaSource` pass
      to the `FileSource` constructor, today the identity
      `(sourceData, resourceType) -> sourceData`. `FileSource.load` applies it after
      `LayoutDetector.check`. `FileSource.load` itself is not changed, because `NdjsonSource` and `BundlesSource` share it and their
      transformers (`context.encode`, `context.encodeBundle`) already shape the data with the
      defined-schema reader. `ParquetSource.readerFor` stops binding the schema with
      `.schema(...)`.
    - **Catalog tables**: `CatalogSource.read`, after `LayoutDetector.check` and before
      `universalOperator`.
    - **Datasets**: `DatasetSource.dataset(type, df)`, where a dataset is registered. Not
      `DatasetSource.read`, which `FileSource` inherits, so projecting there would shape NDJSON
      and bundle reads a second time.

- **Rationale**: Every path lands on the same schema (FR-009). The projector already exists in
  Column-API form, so `io` may hold it. Recomputing annotations satisfies FR-007 and covers a
  stored table annotated under another configuration. The dropped paths are known from the schemas
  alone for stored data, so logging them is free, and so is the check of the stored schema that #2884 adds.
  NDJSON, bundle and XML reads with a defined schema run no inference, so the content they drop
  could be found only by per-row work; they do not look for it, and so do not fail on content a
  read without a schema would fail on.
- **Alternatives considered**: `MergeCast.project` uses the Catalyst API and lives in `encoders`.
  `spark.read.schema()` on Parquet (the current `parquetWithSchemas`) cannot reorder nested
  structs or drop nested fields reliably, and does not apply to Delta tables or datasets.

- **Cost**: A Parquet read with a defined schema still merges every file's footer
  (`mergeSchema=true`), because the stored schema is checked and projected rather than bound
  with `.schema(...)`. Today's Parquet `schemas` option binds the read schema and so skips that
  merge; a defined schema loses the saving, and on a lake of many files costs the same read as
  one without a schema, plus a fixed output shape. The release notes state this (FR-022a). Reading with the layout schema as the Parquet
  read schema and then projecting would skip the merge without changing the output, but it would
  also skip the stored-schema check of step 1, so it is left as a later optimisation.

## R7. API shape

- **Decision**: a schema is a Spark `StructType` and a schema set is a plain map of them, in each
  binding's own idiom. JSON conversion is by functions, not by a class wrapping the set.
    - **Java**:
        - `DataSourceBuilder.withSchemas(Map<String, StructType>)` returns a builder whose every
          source method applies the set.
        - `encode` and `encodeBundle` overloads with a trailing `@Nullable StructType schema`.
        - `QueryableDataSource.schemas()` returns `Map<String, StructType>`.
        - `PathlingContext.canonicalSchemas(Map<String, StructType>)` returns the same type
          (FR-027).
        - A small public utility class in `library-api`, `SchemaSets`, converts a set to and from
          the schema set file's content: `@Nonnull String toJson(@Nonnull Map<String, StructType>)`
          and `@Nonnull Map<String, StructType> fromJson(@Nonnull String)` (FR-028).
    - **Python**: a `schemas=` keyword, taking `dict[str, StructType]`, on each `read.*` method; a
      `schema=` keyword, taking a `StructType`, on `encode` and `encode_bundle`;
      `DataSource.schemas()` and `pc.canonical_schemas(schemas)` returning `dict[str, StructType]`;
      and `write_schemas(schemas, path)` and `read_schemas(path)` in the `pathling` package. The
      two file functions are pure Python, over `StructType.json()` and `StructType.fromJson`, and
      need no JVM.
    - **R**: a `schemas` argument on each `pathling_read_*` function, a `schema` argument on
      `pathling_encode*`, `ds_schemas()`, `pathling_canonical_schemas(pc, schemas)`, and
      `pathling_write_schemas(sc, schemas, path)` and `pathling_read_schemas(sc, path)`. sparklyr
      has no `StructType`, so in R a schema is a string holding Spark's JSON form and a schema set
      is a named list of such strings. The strings are opaque to the user and cross to the JVM as
      they are, so no R list is ever converted to schema JSON, and the empty-`metadata` problem of
      `jsonlite` (an empty unnamed `list()` written as `[]`, where Spark expects `{}`) cannot
      arise. The file functions take a Spark connection and delegate to Java's
      `SchemaSets.fromJson` and `SchemaSets.toJson`, so no schema JSON is parsed or written in
      R.

    Every binding reads and writes the one schema set file format of data-model.md, so a file
    written in one is read by the others.

- **Rationale**: `StructType` is what PySpark and Java users already hold: `df.schema` gives one,
  and Spark's own readers take one. Builder methods are the Java idiom (`withLayoutDetection`), and
  keywords the Python and R idiom. A set the user keeps in a file is met by the write and read
  functions, so the round trip (FR-019) needs no JSON form on the readers. R's strings cross
  sparklyr without a type conversion, and that removes its 10-entry `Map.of` cap
  (`to_java_schema_map`).
- **Alternatives considered**:
    - Spark's JSON form only, on every binding (the previous decision). Rejected: it is unidiomatic
      for PySpark and Java users, who already hold `StructType`s and would convert every one, and
      its reason, that a `StructType` does not round-trip through a file the user keeps, is met by
      the write and read functions.
    - A public `SchemaSet` class wrapping the set, with its own JSON reading, writing and
      canonicalisation. Rejected: more API surface than needed, since plain maps and a few
      functions do the same.

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
    `foreachBatch`, not through Spark's streaming file sinks. The read-back that User Story 2
    promises is of that output, and a `StreamingEncodeTest` case writes a stream through
    `foreachBatch` to Delta and to Parquet and reads each back through Pathling.

## R10. Schema set parsing and errors

- **Decision**: `io` `SchemaSet` is the internal form of a schema set, behind the public API.
  `SchemaSet.of(Map<String, StructType>)` takes the map that `withSchemas` and `canonicalSchemas`
  are given, and `SchemaSet.parse(String json)` parses a schema set file's content, each entry
  with `DataType.fromJson`; `SchemaSets.fromJson` delegates to it, and the R binding joins its
  strings into one such object and passes it to `SchemaSets.fromJson`. Neither is public API. They refuse, with an error naming the key:
    - a key that is not a resource type code known to the definitions, when the set is given to a
      source or to `canonicalSchemas`;
    - a value that is not a struct;
    - a value that is malformed.

    A malformed file is refused by `read_schemas` and `pathling_read_schemas` with an error naming
    the file, and malformed content by `SchemaSets.fromJson`. An empty set equals no set.

- **Rationale**: These are the spec's edge cases. One parser serves every entry point. The
  resource type check needs the definitions, so it is made where a context is at hand, not by
  `SchemaSets.fromJson` or `read_schemas`, which have none.

## R11. Testing strategy (ADR-0007)

- **Decision**:
    1. **Load tests** (FR-021a, SC-003): for every source and encode path, the output schema
       equals the given PoF base schema plus the configured annotations, including for batches
       missing schema elements and batches carrying extra elements. "Equals" is the schema
       equality of spec SC-002, implemented once per binding as a test helper (`SchemaEquality`
       in Java, `schema_equality.py` in Python, `helper_schema_equality.R` in R).
    2. **Path parity** (FR-009, SC-004): JSON = XML = stored Parquet.
    3. **Absence** (FR-021, SC-005): extend `AbsentElementTest` and its peers to any position
       kind not yet covered. The survey found:
        - existing coverage of top-level, nested, choice, metadata-group, annotation, recursion
          and one-sided combinations;
        - gaps in a repeating complex element absent on both sides of a combination, and an
          absent primitive metadata group inside a repeating complex element.
    4. **YAML suites on defined reads** (FR-021b), in the regular build, for both
       `YamlReferenceImplTest` (fhirpath.js) and `YamlFhirPathTest` (ptl). Today a case loads
       one of two ways (`YamlTestArgumentProvider`, lines 137–197):
        - **Path 1**: a subject that is a FHIR resource (an `inputfile` from the suite's resource
          directory, such as the 19 files of `fhirpath/src/test/resources/fhirpath-js/resources/`,
          or an inline subject with a valid `resourceType`) goes through `FhirResolverFactory` to
          `PofDatasets.fromJson`, and so to `FhirReader.json().read(type, Dataset<String>)` with
          inference.
        - **Path 2**: any other subject goes through `ArbitraryObjectResolverFactory`, which
          builds a definition from the YAML (`YamlSupport.yamlToDefinition`) and a hand-built
          stored schema (`YamlSupport.definitionToStruct(def, subject)`, with `storedType` and
          `storedCodingType`), and reads with `spark.read().schema(...).json(...)`, bypassing
          `FhirReader`.

        The new design changes both:
        - **Path 2** reads through
          `FhirReader.of(spark, DefaultDefinitionContext.of(subjectDefinition), config).json().read(code, docs, definitionToStruct(subjectDefinition))`:
          a defined read with the schema derived from the definition alone. `FhirReader` accepts
          any `DefinitionContext`. The `YamlSupport` helpers that served only the hand-built
          stored layout (`storedType`, `storedCodingType`, the subject-pruned
          `definitionToStruct` overload, and `subjectToJson` if no longer needed) are removed once
          a check of their usages, including `YamlTestRunnerTest`, shows nothing else needs them.
        - **Path 1** loads each case with a defined schema: the schema inferred from all of the
          suite's input resources of the case's type, merged by name with the case's own inferred
          schema, so it is always at least as wide as the data. It is derived at test time and
          cached per suite and type. Nothing is checked in. The inferred load stays covered by
          the `io` and `library-api` tests.

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

## R14. Field order of the output

- **Decision**: The output of a load with a defined schema keeps the defined schema's field order at
  every depth. The ingest walk builds canonical order, so its output is reordered by name into the
  defined order, with the by-name projector of R6; for a canonical schema this is a no-op, because
  the projector leaves a struct that already has the target type untouched. Fields the
  configuration adds keep their canonical place: annotations last in their struct, a metadata group
  holding only annotations right after its primitive. A canonicalisation function (FR-027) sorts a
  schema set into canonical field order using the canonical structure of `fhir-schema`, without
  reading data.
- **Rationale**: Nothing in Pathling depends on field order: sinks, decoding, FHIRPath combining and
  every storage write match by name ([field-order.md](evidence/field-order.md)). Order matters only
  to positional operations users run against an existing table, whose order is often not canonical
  because storage keeps the first writer's order and evolution adds fields last. Keeping the given
  order makes such a load line up with that table with no further step, and matches Spark's own
  `spark.read.schema(...)` and `from_json`. A user who wants canonical order canonicalises the
  schema once, when it is written.
- **Alternatives considered**: Always produce canonical order. It keeps "every Pathling struct is
  canonical" (FR-057 of 001), but a user matching an existing non-canonical table would need
  `df.to(schema)` after every load. `Dataset.to` was confirmed to reorder nested structs, arrays and
  maps of structs by name, so it would work, but it is a step on every load in place of one on each
  schema. The cost of the decision is that FR-057 of 001 and ADR-0002 gain an exception, and two
  lakes loaded with schemas in different orders differ physically.

## R15. Nullability of the output

- **Decision** (owner, 2026-11-27): The given schema's nullability is ignored. Each field keeps the
  nullability that the same source gives without a schema: nullable from JSON, XML, NDJSON,
  bundles, bulk exports and Parquet files, and the stored flags from Delta tables, catalog tables
  and datasets. Stored flags pass through subtrees that the projection rebuilds to match the
  given schema: a struct that cannot be null is rebuilt without a null branch, so it stays
  `NOT NULL`, and so do the elements of an array with `containsNull=false` and the values of a map
  with `valueContainsNull=false` (PR #3017 review). The one widening is a `NOT NULL` field inside a
  nullable struct that is rebuilt, which comes out nullable, because Spark makes any field reached
  through a nullable struct nullable. A field that only the given schema has, and the source lacks,
  is nullable. Field metadata is unchanged from FR-008: the given field metadata is not carried.
  FR-008 is amended to say so.
- **Rationale**: Spark's file and JSON readers reset every flag to nullable, while Delta, catalog
  tables and datasets keep theirs, and the projection of R6 passes the stored flags through. A flag
  that says `NOT NULL` where the data holds a null gives wrong results: the optimiser folds
  `isNull` to false, and a null array element can come back as 0 or as another element
  ([evidence/nullability.md](evidence/nullability.md), sections 2 and 5). Passing the source's own
  flags through never marks a field `NOT NULL` that the source did not already guarantee. The
  spike measured the native readers and its own `Conform` projection, not the projection of R6;
  that the projection keeps the stored flags, including in rebuilt subtrees, is established by the
  nullability cases of `ProjectionTest`.
- **Alternatives considered**:
    - Every field nullable, as FR-008 first said. It needs widening on every stored read, with
      `when(c.isNotNull(), v)` at every depth or a cast of the whole row, and it discards
      guarantees that Delta tables and datasets give.
    - Exact nullability taken from the given schema, with a check at load time. The spike shows it is
      buildable with the public API, batch and streaming, but the check runs only where its column is
      evaluated, so `count()` or a narrower select skips it and a downstream `filter(isNull)` silently
      returns nothing. It also needs Delta sinks to create tables with the nullable form, because
      Delta does not check array elements or map values. A load-time row check for `NOT NULL` in the
      given schema may follow as a later slice.

## R16. Case-only name differences in stored data

- **Decision** (owner, 2026-11-26, triage of #3007): A stored field whose name differs from the
  given schema's only in case, or a stored struct holding two fields whose names differ only in
  case, fails the read with an error naming the resource type and both paths, as the edge case in
  `spec.md` says. The check covers the structs the given schema keeps. A stored subtree that the
  schema drops is not inspected, so a dataset with both `name.family` and `name.Family`, read with
  a schema that omits `name`, is accepted. `Projection` makes the check as a separate first step, so the check that #2884
  adds for undescribed fields can run after it.
- **Rationale**: FHIR names are case-exact. Without the check, a stored `Family` could be taken for
  `family`, or dropped as content the schema has no field for. Either way the data would change
  silently.
- **Alternatives considered**: Matching case-insensitively, as Spark does, which reads a misnamed
  field as the FHIR element. Dropping the field as content the schema has no field for, which loses
  it without a word.

## R17. Case-only duplicate columns in Parquet and Delta files

- **Decision** (owner, 2026-11-26): A Parquet or Delta file whose columns include two names that
  differ only in case, such as `id` and `ID`, fails with Spark's own `COLUMN_ALREADY_EXISTS` error,
  untranslated.
- **Rationale**: Spark refuses such a file while it loads it, before the source sees any data, with
  or without a schema set. This holds at the top level, in nested structs, in arrays of structs and
  in map values. Spark's error names only one of the columns.
- **Alternatives considered**: Translating Spark's error into Pathling's case-only error of R16. It
  could name only the column Spark reports, unless the source re-read the file footers. Moving the
  clash into a nested struct to reach R16's check does not help, because Spark refuses it there too.

## R18. Case-only clash in a Delta merge

- **Decision** (owner, 2026-11-26, gate 03 G2): A merge by `DeltaSink` into a table that has a field
  whose name differs from a field of the resources only in case is an error, with or without a
  schema set. The message is prefixed "Cannot merge <Type> into its table" and names both paths.
- **Rationale**: The merge conforms the resources to the table with `Projection`, which makes the
  check of R16. A case-insensitive match would write a FHIR element into a misnamed column.
- **Alternatives considered**: Leaving the merge as it was, with no case check.

## R19. Top-level names containing a dot or a backtick

- **Decision** (owner, 2026-11-26, triage of #3007): Won't fix. `Projection` does not support
  top-level names that contain a dot or a backtick.
- **Rationale**: FHIR element names and Pathling's annotation names never contain either character,
  so no FHIR data reaches the case.
- **Alternatives considered**: Escaping such names, which adds code for data that cannot occur.

## R20. What `Projection.project` leaves as stored

- **Decision** (owner, 2026-11-26, triage of #3007): `Projection.project` does not apply the target's
  nullability or field metadata, and where a leaf's stored type differs from the target's it keeps
  the stored type. This is documented in its Javadoc, not changed. For nullability, see R15.
- **Rationale**: The projection selects and orders fields by name and casts nothing, as the edge
  case on stored types in `spec.md` says. Field metadata of the output is that of an inferred load
  (FR-008), which `Annotator` gives after the projection.
- **Alternatives considered**: Applying the target's flags, metadata and leaf types in the
  projection. Applying the flags is rejected in R15. Casting leaves would change values, which the
  specification rules out.

## R21. Schema sets from data: decisions at gate 04 G2

- **Decision** (owner, 2026-11-27, gate 04 G2):
    - Canonicalisation keeps the nullability and field metadata of each field it is given, and
      changes only the order of the fields and which it drops.
    - `SchemaSets.fromJson` keeps the order of the keys of the file, and so does `SchemaSet`.
    - `QueryableDataSource.schemas()` is a default method, not abstract as
      `contracts/java-api.md` first said: it reads the schema of each type `getResourceTypes()`
      names, in that order, into a `LinkedHashMap`. It throws nothing of its own.
    - The refusal by canonicalisation of a field whose type or cardinality contradicts the
      definitions (FR-027) is made in slice 04, as a by-product of the walk that orders the
      fields. It is used only by `canonicalSchemas` and is never part of a read. Slice 07
      reconciles it with the check that #2884 adds.
    - A schema set file that names a resource type twice is refused in every binding, naming the
      type, as the edge case in `spec.md` says. Python's `json` keeps the last value of a
      repeated key, so `read_schemas` detects the repeat itself.
- **Rationale**: Canonicalisation is asked for order, so it changes nothing else. A file is the
  user's, and its order is kept as the order of a set given in code is (R14). A default method
  serves every implementation of the interface, including the server's, without each repeating
  it. The contradiction refusal costs nothing in the walk, and a check made in a read is what
  slice 07 is for.
- **Alternatives considered**: An abstract `schemas()` implemented in `AbstractSource`, which
  breaks every other implementation of the interface. Leaving the contradiction refusal to slice
  07, which would leave canonicalisation to keep or drop a field it cannot place.
