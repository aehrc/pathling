# Tasks: Load FHIR resources with a schema defined up front

**Input**: Design documents in `specs/2881_defined-schemas/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a test task that
precedes its implementation task, and those tests must be written to fail first.

**Organisation**: Tasks are grouped by user story so each story can be implemented and tested
independently.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: can run in parallel (different files, no dependency on incomplete tasks).
- **[Story]**: the user story a task belongs to (US1, US2 or US4; US3 was removed); omitted for
  Setup, Foundational, Engine parity and Polish.
- Paths are relative to the repository root. `io/…/io/` is
  `io/src/{main,test}/java/au/csiro/pathling/io/`, and `library-api/…/library/` is
  `library-api/src/{main,test}/java/au/csiro/pathling/library/`.

---

## Phase 1: Setup

**Purpose**: Settle the two facts the read rests on, and the open product question.

- [ ] T001 Create the evidence index `specs/2881_defined-schemas/evidence/README.md`, listing each
      measurement and the decision it supports.
- [ ] T002 [P] Measure whether each step of a defined read needs an action, or fails, over a
      streaming dataset. Record a runnable reproduction and the result of each in
      `specs/2881_defined-schemas/evidence/stream-safety.md` (research R4, R5):
    - `spark.read().json(Dataset<String>)` with a given schema;
    - `from_json` over a streaming dataset;
    - the bundle splitter UDF (`BundleResource.resources` with `FhirJsonReader`'s and
      `FhirXmlReader`'s `SPLIT`) followed by `explode`, over a streaming dataset;
    - `from_xml` over a streaming dataset.
- [ ] T003 [P] Confirm which Spark type the inferred JSON read, with `FhirJsonReader`'s options,
      gives a FHIR decimal. Record the reproduction and the result in
      `specs/2881_defined-schemas/evidence/decimal-read.md`, and name that type in
      `specs/2881_defined-schemas/research.md` R3. The defined read uses the same type (parity
      wins over the lexical form).
- [x] T004 [P] Confirm with the Pathling owner how a resource type that the schema set names but
      the source lacks should behave. Decided: today's behaviour is kept, and an empty table is the
      follow-up #2997. No tasks are added.

---

## Phase 2: Foundational (blocking prerequisites)

**Purpose**: The schema set, the projector, and the defined-schema read in `io`, which every story
uses.

### Tests ⚠️ write first, confirm failing

- [ ] T005 [P] Test schema-set parsing in `io/…/io/SchemaSetTest.java`:
    - parsing from a JSON string and from a map;
    - an unknown resource type key is refused with an error naming the key;
    - a malformed or non-struct schema is refused with an error naming the type;
    - an empty set equals no set.
- [ ] T006 [P] Test the by-name projector in `io/…/io/ProjectionTest.java`:
    - extra fields are dropped and missing fields are added as typed nulls, at the top level and
      inside structs, arrays and maps;
    - a null struct stays null;
    - field order follows the target.

    A leaf type mismatch is left untested here; #2884 decides it.

- [ ] T007 [P] Test observed-shape and layout-schema derivation in
      `io/…/io/DefinedSchemaLogicTest.java`:
    - each primitive kind maps to its ingest type, with decimal as fixed by T003;
    - metadata groups, choice types, and repeating and singular complex elements are mapped;
    - annotation fields are skipped;
    - undescribed fields are dropped at the top level and nested, with no exception;
    - the layout schema equals the base schema for a PoF schema (FR-008);
    - a non-PoF schema yields a PoF schema.
- [ ] T008 [P] Test defined-schema reads in `io/…/io/DefinedSchemaReadTest.java`, for JSON files,
      JSON documents, JSON bundles, XML documents and XML bundles:
    - elements the batch lacks are present and null;
    - content the schema lacks is dropped with no error (FR-006);
    - annotations follow the configuration, whatever annotations the schema carries (FR-007);
    - metadata groups follow the schema whatever the primitive metadata option (edge case);
    - a read without a schema is unchanged (FR-020).
- [ ] T009 [P] Test path parity in `io/…/io/DefinedSchemaParityTest.java`:
    - the same resources as JSON, as XML, and as stored PoF Parquet projected through `Projection`
      and `Annotator`, with the same schema, give equal data (FR-009, SC-004);
    - a defined load whose schema equals the inferred schema gives the inferred load (FR-021a);
    - JSON bundles and XML bundles read with and without a schema give equal data, including
      `urn:uuid` references between entries resolved by `BundleResource.resolve` (research R4).
- [ ] T010 [P] Test that the defined reads of a streaming dataset (a `MemoryStream` of strings)
      return a streaming dataset with the layout schema, in
      `io/…/io/DefinedSchemaStreamingReadTest.java`, for JSON documents, XML documents, JSON
      bundles and XML bundles.
- [ ] T011 [P] Test the schema-equality helpers of spec SC-002, one per binding, in
      `library-api/src/test/java/au/csiro/pathling/library/io/SchemaEqualityTest.java`,
      `lib/python/tests/test_schema_equality.py` and
      `lib/R/tests/testthat/test-schema-equality.R`:
    - schemas that differ only in nullability (of fields, array elements and map values) or in
      field metadata, at the top level and nested, are the same;
    - schemas that differ only in annotation fields are the same;
    - a missing, extra, renamed or retyped field, or a different field order, makes them differ;
    - two schema sets are the same when they name the same types and each pair is the same.

### Implementation

- [ ] T012 [P] Implement `SchemaSet` (`parse(String)`, `of(Map<String, String>)`, lookup by type,
      validation against the definitions) in `io/src/main/java/au/csiro/pathling/io/SchemaSet.java`.
- [ ] T013 [P] Implement `Projection.project(Dataset<Row>, StructType)` in
      `io/src/main/java/au/csiro/pathling/io/Projection.java`, extracted from
      `DeltaSink.conformToTarget`/`conformColumn`/`conformFields`.
- [ ] T014 Replace `DeltaSink`'s private conform methods with `Projection` in
      `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`. Run
      `NarrowMergeTest` and `DeltaSchemaMergingTest` unchanged.
- [ ] T015 Implement the observed-shape derivation, skipping undescribed and annotation fields, in
      `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java`. On a defined-schema read
      the walk keeps or drops each primitive metadata group as the schema says and does not
      consult `storePrimitiveMetadata` (today checked in `ResourceTransformer`'s primitive and
      repeating-element paths), so that T008's metadata case passes. Implement `observedShape(type,
schema)` and `layoutSchema(type, schema)` in
      `io/src/main/java/au/csiro/pathling/io/DefinedSchemaLogic.java`.
- [ ] T016 Add defined-schema overloads of `read(type, path)`, `read(type, documents)` and
      `readBundles(type, bundles)`:
    - declare them in `io/src/main/java/au/csiro/pathling/io/FhirFormatReader.java`;
    - implement them in `io/src/main/java/au/csiro/pathling/io/FhirJsonReader.java` with
      `from_json`, or `spark.read().schema(...)` for files, and no inference (research R4);
    - the bundle overload keeps the splitter UDF, the `explode` and the reference resolution of
      `readBundles`, passing `reference -> BundleResource.resolve(reference, references)` to
      `toLayout`, and drops only the inference read.
- [ ] T017 Implement the XML overloads in `io/src/main/java/au/csiro/pathling/io/FhirXmlReader.java`,
      building the parse schema from the observed shape in
      `io/src/main/java/au/csiro/pathling/io/XmlIngestShape.java` and skipping the inference read
      (research R5). The bundle overload keeps the splitter UDF, the references column and the
      `BundleResource.resolve` call of the private `toLayout`, as the inferred bundle read does.
- [ ] T018 [P] Implement the schema-equality helpers that T011 tests:
      `library-api/src/test/java/au/csiro/pathling/library/io/SchemaEquality.java` (removing
      annotation fields with `AnnotationNames.isAnnotation`),
      `lib/python/tests/schema_equality.py` (`same_schema`, `same_schema_set`, over the JSON form)
      and `lib/R/tests/testthat/helper_schema_equality.R`, which testthat loads before every test
      file.

**Checkpoint**: T005–T011 pass. `mvn install -pl io -am` is green.

---

## Phase 3: User Story 1 - Load and write with a defined schema (Priority: P1) 🎯 MVP

**Goal**: Every source, in every binding, takes a schema set and lands on its schema, and every
sink keeps that schema when it writes.

**Independent Test**: Three NDJSON batches of differing content, each read with a lake's schema
set and appended to Delta, leave the table's schema equal to the set.

### Tests for User Story 1 ⚠️ write first, confirm failing

- [ ] T019 [P] [US1] Test every source with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/DefinedSchemaSourceTest.java`.
      The sources are ndjson, bundles, parquet, delta, tables, bulk (through the existing bulk test
      fixture) and datasets. For each:
    - the exact base schema, compared with `SchemaEquality`, plus the configured annotations,
      for batches missing schema elements and batches carrying extra ones (FR-001,
      FR-006–FR-008, SC-003);
    - a partial set projects the named types and infers the rest (FR-010);
    - for ndjson and bundles (JSON and XML), the dataset returned for a named type is shaped once:
      it has the same analysed plan (`sameResult`) as the `io` defined read of T016 or T017 on
      the same input with the same schema, with no `Projection` or `Annotator` step on top;
    - `withSchemas` accepts a JSON string and a map (FR-005);
    - `parquetWithSchemas` is gone (FR-003).
- [ ] T020 [P] [US1] Test writing data loaded with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/sink/DefinedSchemaWriteTest.java`:
    - parquet, delta and tables write each named type with the set's schema, compared with
      `SchemaEquality` (US1 scenario 5, SC-003);
    - three Delta appends of differing batches, each loaded with the table's schema set under the
      annotation configuration the table was written with, keep the table's schema by
      `SchemaEquality` (SC-002);
    - a Delta merge of data loaded with a narrower schema nulls the missing fields of matched rows
      (edge case);
    - an append of data whose schema differs from the table's widens it (edge case).
- [ ] T021 [P] [US1] Test the Python sources with schema sets given as a dict and as a
      JSON string, including more than ten types, and the refusal of a Spark `StructType` (FR-004),
      in `lib/python/tests/test_defined_schemas.py`. Compare output schemas with
      `same_schema_set` from `lib/python/tests/schema_equality.py`.
- [ ] T022 [P] [US1] Test the R sources with schema sets given as a named list and as a
      JSON string, including more than ten types, in `lib/R/tests/testthat/test-defined-schemas.R`.
      Compare output schemas with the helper of `lib/R/tests/testthat/helper_schema_equality.R`.

### Implementation for User Story 1

- [ ] T023 [US1] Add `withSchemas(String)` and `withSchemas(Map<String, String>)`, remove
      `parquetWithSchemas`, and pass the parsed `SchemaSet` to every source constructor, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DataSourceBuilder.java`.
- [ ] T024 [US1] Project each named type, with `Projection` then `Annotator`, in the transformer
      function that `ParquetSource` and `DeltaSource` pass to the `FileSource` constructor, today
      the identity `(sourceData, resourceType) -> sourceData`, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/ParquetSource.java` and
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DeltaSource.java`.
      `FileSource.load` applies it after `LayoutDetector.check` and is not changed, since
      `NdjsonSource` and `BundlesSource` share it (research R6). Drop the `.schema(...)` binding
      from `ParquetSource.readerFor`. Log at debug level the paths of the stored schema that the
      defined schema drops, found by comparing the two schemas only, with no Spark job and no
      per-row work (FR-006a). There is no dedicated logging test.
- [ ] T025 [P] [US1] Project each named type the same way, logging the dropped paths at debug
      level as T024 does (FR-006a): in `CatalogSource.read`, after `LayoutDetector.check` and
      before `universalOperator`, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/CatalogSource.java`; and in
      `DatasetSource.dataset(type, df)`, where a dataset is registered, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DatasetSource.java`. Not in
      `DatasetSource.read`, which `FileSource` inherits, so that NDJSON and bundle reads are not
      shaped twice.
- [ ] T026 [P] [US1] Read each named type with the defined-schema reader in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/NdjsonSource.java`,
      `BundlesSource.java` and `BulkDataSource.java` (same directory). These reads do not detect
      or log dropped content (FR-006a).
- [ ] T027 [US1] Add the `schemas=` keyword, accepting a dict or a JSON string, to every reader in
      `lib/python/pathling/datasource.py`. Replace the `StructType` form of `parquet(..., schemas=)`.
      Place `schemas` before `layout_detection` on `parquet`, `delta` and `tables`, as in
      `contracts/python-api.md`.
- [ ] T028 [US1] Add the `schemas` argument, accepting a named list or a JSON string, to every
      reader in `lib/R/R/datasource.R`. Replace `to_java_schema_map` and its `Map.of`
      cap with a JSON string conversion that writes an empty field `metadata` as `{}`, not `[]`
      (the hand-built case of T038). Place `schemas` before `layout_detection` on the parquet,
      delta and tables readers, as in `contracts/r-api.md`.

**Checkpoint**: T019–T022 pass. US1 independent test passes.

---

## Phase 4: User Story 2 - Encode a stream with a defined schema (Priority: P1)

**Goal**: `encode` and `encodeBundle` take a schema and accept streams. Encoded streams written to
Delta and Parquet are read back by Pathling.

**Independent Test**: A streaming NDJSON source encoded with a schema, written to Delta and
Parquet, is read back through Pathling.

### Tests for User Story 2 ⚠️ write first, confirm failing

- [ ] T029 [P] [US2] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/StreamingEncodeTest.java`:
    - `encode` of streaming JSON and XML, and `encodeBundle` of streaming bundles, with a schema
      return a streaming dataset with the layout schema (FR-012);
    - without a schema, the encode is refused with `UnsupportedOperationException`, as today, and
      the message of research R12, which keeps the `foreachBatch` recommendation (FR-013);
    - a batch `encode` with a schema gives the schema's output (FR-002).
- [x] T030 Removed with FR-014: the read-back of a raw `writeStream` is not tested.
- [ ] T031 [P] [US2] Test streaming `encode` and `encode_bundle` with a schema, and the refusal
      without one, in `lib/python/tests/test_encoders.py`.
- [ ] T032 [P] [US2] Test streaming `pathling_encode` and `pathling_encode_bundle` with a schema,
      and the refusal without one, in `lib/R/tests/testthat/test-encoding.R`.

### Implementation for User Story 2

- [ ] T033 [US2] Add the `encode` and `encodeBundle` overloads with a schema, route them to the
      defined-schema reader, and allow streams only when a schema is given, keeping
      `UnsupportedOperationException` with the new refusal message, in
      `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`.
- [x] T034 Removed with T030.
- [ ] T035 [P] [US2] Add the `schema=` keyword to `encode` and `encode_bundle` in
      `lib/python/pathling/context.py`.
- [ ] T036 [P] [US2] Add the `schema` argument to `pathling_encode` and `pathling_encode_bundle` in
      `lib/R/R/encoding.R`.

**Checkpoint**: T029–T032 pass.

---

## Phase 5: User Story 4 - Take a schema set from existing data (Priority: P2)

**Goal**: Every source returns its schema set, and the set round-trips through JSON into every
source.

**Independent Test**: For every source, `schemas()` round-tripped through a file is accepted by
every source.

### Tests for User Story 4 ⚠️ write first, confirm failing

- [ ] T037 [P] [US4] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/SourceSchemasTest.java`:
    - `schemas()` on every source has one entry for each type held (FR-017);
    - a Parquet source with differing files returns the merged schema (FR-018);
    - a set written to a JSON file and read back is accepted by every source, and each source's
      output schema set is the same as it by `SchemaEquality` (FR-019, SC-007);
    - a single entry is accepted by `encode` and `encodeBundle` (FR-005).
- [ ] T038 [P] [US4] Test `DataSource.schemas()` round trips through `json.dump`/`json.load` in
      `lib/python/tests/test_defined_schemas.py`, and `ds_schemas()` through `jsonlite` in
      `lib/R/tests/testthat/test-defined-schemas.R`, comparing with each binding's schema-equality
      helper. The R test also passes a hand-built schema, written as an R list with empty field
      metadata, to a source, because `jsonlite` serialises an empty unnamed `list()` as `[]`
      where Spark expects `"metadata": {}`.

### Implementation for User Story 4

- [ ] T039 [US4] Add `schemas()` to
      `library-api/src/main/java/au/csiro/pathling/library/io/source/QueryableDataSource.java`, and
      implement it once in `AbstractSource.java` (same directory).
- [ ] T040 [P] [US4] Add `DataSource.schemas()` in `lib/python/pathling/datasource.py` and
      `ds_schemas()` in `lib/R/R/datasource.R`.

**Checkpoint**: T037–T038 pass.

---

## Phase 6: Engine parity (FR-021, FR-021b)

**Purpose**: Show that query results do not depend on which subset of the uber-schema is stored
(ADR-0007).

- [ ] T041 Map every position kind of SC-005 to the existing absence tests named in the survey,
      and record the matrix and its gaps in `specs/2881_defined-schemas/evidence/absence-coverage.md`.
- [ ] T042 Add failing-first tests for each gap found by T041 in
      `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`. The gaps include:
    - a repeating complex element absent on both sides of a combination;
    - a primitive metadata group absent inside a repeating complex element.

    Fix any engine defect they expose.

- [ ] T043 Record the baseline: run `YamlReferenceImplTest` and `YamlFhirPathTest` with today's
      loads, and record each case's result in
      `specs/2881_defined-schemas/evidence/yaml-defined-run.md`, under a baseline heading.
- [ ] T044 [P] Test the load of a case whose subject is not a FHIR resource (path 2) in
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/resolver/ArbitraryObjectResolverFactoryTest.java`:
      for subjects with primitive, complex, repeating and missing elements, the resolved dataset
      equals the defined read of `FhirReader` with the schema derived by `definitionToStruct` from
      the subject's definition alone, and keeps the subject's values (FR-021b).
- [ ] T045 [P] Test the load of a case whose subject is a FHIR resource (path 1) in
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/resolver/FhirResolverFactoryTest.java`:
    - the defined schema is the schema inferred from all of the suite's input resources of the
      case's type, merged by name with the case's own inferred schema;
    - it is at least as wide as the case's inferred schema, for an input file and for an inline
      subject;
    - it is derived once per suite and type;
    - the resolved dataset keeps the case's values (FR-021b).
- [ ] T046 Read path 2 through
      `FhirReader.of(spark, DefaultDefinitionContext.of(subjectDefinition), config).json().read(code, docs, definitionToStruct(subjectDefinition))`
      in
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/resolver/ArbitraryObjectResolverFactory.java`.
      Check the usages of the `YamlSupport` helpers that served only the hand-built stored layout
      (`storedType`, `storedCodingType`, the subject-pruned `definitionToStruct` overload, and
      `subjectToJson`), including in
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/YamlTestRunnerTest.java`, and remove from
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/YamlSupport.java` those nothing else
      uses.
- [ ] T047 Load path 1 with a defined schema: derive each suite's schema per resource type from all
      of its input resources (the `inputfile`s of its resource directory and its inline FHIR
      subjects), cache it per suite and type, and merge it by name with each case's inferred
      schema, in
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/YamlTestArgumentProvider.java` and
      `fhirpath/src/test/java/au/csiro/pathling/test/yaml/resolver/FhirResolverFactory.java`. Nothing
      is checked in, and no profile or `TestSchemaMode` mode is added. The inferred load stays
      covered by the `io` and `library-api` tests, and narrowing (FR-006) by the load tests, since
      these schemas are only ever wider than the data.
- [ ] T048 Run `YamlReferenceImplTest` and `YamlFhirPathTest` in the regular build and compare each
      case with the baseline of T043. Fix each changed result as an engine or reader defect or, if
      it is a genuine conformance gap, record it in
      `fhirpath/src/test/resources/fhirpath-js/config.yaml` or
      `fhirpath/src/test/resources/fhirpath-ptl/config.yaml` per the `pathling-yaml-exclusions`
      skill. Record the comparison and each disposition in
      `specs/2881_defined-schemas/evidence/yaml-defined-run.md` (FR-021b, SC-005).

---

## Phase 7: Polish & cross-cutting concerns

- [ ] T049 [P] Rewrite the schema documentation for defined schemas in every binding in
      `site/docs/libraries/io/schema.md`:
    - getting, saving and passing a schema set;
    - loss of content outside the schema with no error (logged at debug level only for stored
      sources, FR-006a), and wide schemas as the remedy;
    - annotations follow the configuration, and metadata follows the schema;
    - Delta merge narrowing;
    - output equals the given schema only for a PoF schema;
    - bootstrapping a set from a sample (FR-022, FR-023).

    Replace the "streaming dataset cannot be encoded" paragraph and the "fitted to the data" section.

- [ ] T050 [P] Document the `schemas` option on sources, and that sinks keep the source's schema,
      in
      `site/docs/libraries/io/index.md`.
- [ ] T051 [P] Rewrite `site/docs/libraries/examples/streaming.md` with a streaming encode with a
      schema written to Delta and to Parquet through a Pathling sink inside `foreachBatch`, for
      example `pc.read.datasets({...}).write.delta(path, save_mode="merge")`, extending the 10.x
      `foreachBatch` example, in Python and Java. State that a write with Spark's own
      `writeStream` skips the checks a Pathling sink makes (FR-024).
- [ ] T052 Run the streaming example of T051 against a file-based stream and record that it runs
      in `specs/2881_defined-schemas/evidence/streaming-example.md` (SC-008).
- [ ] T053 [P] File GitHub issues for the schema generator (including the 10.x shaping options as
      shortcuts), a schema validator, and Delta idempotent writes, with the PoF_line label. Record
      their numbers under Out of Scope in `specs/2881_defined-schemas/spec.md` (FR-025).
- [ ] T054 [P] Point 001's dense-mode tickets, and its FR-009, FR-010, FR-017, FR-043 streaming
      clause and FR-044, at this specification or the generator issue (FR-026). The files are
      `specs/001-parquet-on-fhir/issues/18-dense-mode.md`,
      `specs/001-parquet-on-fhir/issues/19-dense-parity.md` and `specs/001-parquet-on-fhir/spec.md`.
    - Remove the `dense` placeholder (which refers to M6 and T037b of 001), superseded by this
      specification's defined-schema YAML loads, from
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/TestSchemaMode.java`, with its tests in
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/TestDimensionsTest.java`.
- [ ] T055 Run `mvn install -pl lib/python,lib/R -am` through `scripts/mvn-isolated.sh`, so that
      every existing test passing no schema passes unchanged (FR-020, SC-006).
- [ ] T056 Run the scenarios of `specs/2881_defined-schemas/quickstart.md`.

---

## Dependencies & Execution Order

- **Setup (T001–T004)**: T001 has no dependencies. T002 and T003 feed T007 and T015–T017. T004
  is done and adds no tasks.
- **Foundational (T005–T018)**: depends on T002 and T003. Blocks every story.
    - Tests T005–T011 run in parallel.
    - T012 and T013 run in parallel.
    - T014 follows T013.
    - T015 follows T012.
    - T016 and T017 follow T015.
    - T018 follows T011 and is independent of the rest. T019–T022, T037 and T038 use it.
- **US1 (T019–T028)**: depends on Foundational.
    - T023 precedes T024–T026.
    - T027 and T028 follow T023.
- **US2 (T029–T036)**: depends on Foundational. T035 and T036 follow T033.
- **US4 (T037–T040)**: T037's round trip into sources depends on US1 (T023).
  `schemas()` itself (T039) does not.
- **Engine parity (T041–T048)**: T041, T042 and T043 can start any time. T044 and T045 are written
  and seen to fail before T046 and T047, which depend on T016. T048 follows T046 and T047.
- **Polish (T049–T056)**: the docs follow the APIs they describe (T049 and T050 after US1 and US4,
  T051 after US2). T052 follows T051. T054 follows T047. T055 and T056 come last.

Within each phase, tests are written and seen to fail before the implementation. Tasks on the same
file run sequentially.

## Implementation Strategy

MVP is Setup, then Foundational, then US1: defined schemas on every source. It fixes
downstream streaming and external consumers on its own. Then come US2 (streaming encode), US4,
engine parity, and polish.
