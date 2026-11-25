# Tasks: Load FHIR resources with a schema defined up front

**Input**: Design documents in `specs/2881_defined-schemas/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a test task that
precedes its implementation task, and those tests must be written to fail first.

**Organisation**: Tasks are grouped by user story so each story can be implemented and tested
independently.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: can run in parallel (different files, no dependency on incomplete tasks).
- **[Story]**: the user story a task belongs to (US1–US4); omitted for Setup, Foundational,
  Engine parity and Polish.
- Paths are relative to the repository root. `io/…/io/` is
  `io/src/{main,test}/java/au/csiro/pathling/io/`, and `library-api/…/library/` is
  `library-api/src/{main,test}/java/au/csiro/pathling/library/`.

---

## Phase 1: Setup

**Purpose**: Settle the two facts the read rests on, and the open product question.

- [ ] T001 Create the evidence index `specs/2881_defined-schemas/evidence/README.md`, listing each
      measurement and the decision it supports.
- [ ] T002 [P] Measure whether `spark.read().json(Dataset<String>)` with a given schema, and
      `from_json` over a streaming dataset, need an action. Record a runnable reproduction and the
      result in `specs/2881_defined-schemas/evidence/stream-safety.md` (research R4).
- [ ] T003 [P] Measure what Spark's JSON reader, with `FhirJsonReader`'s options, returns for a
      JSON number read into a string-typed field and into the type inference gives decimals. Record the
      reproduction and the result in `specs/2881_defined-schemas/evidence/decimal-read.md`, and fix the
      choice in `specs/2881_defined-schemas/research.md` R3.
- [ ] T004 [P] Confirm with the Pathling owner how a resource type that the schema set names but
      the source lacks should behave. Record the answer under Open Items in
      `specs/2881_defined-schemas/spec.md`, and add tasks if it changes today's behaviour.

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
    - field order follows the target;
    - a leaf type mismatch is passed through uncast.
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
    - a defined load whose schema equals the inferred schema gives the inferred load (FR-021a).
- [ ] T010 [P] Test that the JSON, bundle and XML defined reads of a streaming dataset (a
      `MemoryStream` of strings) return a streaming dataset with the layout schema, in
      `io/…/io/DefinedSchemaStreamingReadTest.java`.

### Implementation

- [ ] T011 [P] Implement `SchemaSet` (`parse(String)`, `of(Map<String, String>)`, lookup by type,
      validation against the definitions) in `io/src/main/java/au/csiro/pathling/io/SchemaSet.java`.
- [ ] T012 [P] Implement `Projection.project(Dataset<Row>, StructType)` in
      `io/src/main/java/au/csiro/pathling/io/Projection.java`, extracted from
      `DeltaSink.conformToTarget`/`conformColumn`/`conformFields`.
- [ ] T013 Replace `DeltaSink`'s private conform methods with `Projection` in
      `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`. Run
      `NarrowMergeTest` and `DeltaSchemaMergingTest` unchanged.
- [ ] T014 Implement the observed-shape derivation, skipping undescribed and annotation fields, in
      `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java`. Implement `observedShape(type,
schema)` and `layoutSchema(type, schema)` in
      `io/src/main/java/au/csiro/pathling/io/DefinedSchemaLogic.java`.
- [ ] T015 Add defined-schema overloads of `read(type, path)`, `read(type, documents)` and
      `readBundles(type, bundles)`:
    - declare them in `io/src/main/java/au/csiro/pathling/io/FhirFormatReader.java`;
    - implement them in `io/src/main/java/au/csiro/pathling/io/FhirJsonReader.java` with
      `from_json`, or `spark.read().schema(...)` for files, and no inference (research R4).
- [ ] T016 Implement the XML overloads in `io/src/main/java/au/csiro/pathling/io/FhirXmlReader.java`,
      building the parse schema from the observed shape in
      `io/src/main/java/au/csiro/pathling/io/XmlIngestShape.java` and skipping the inference read
      (research R5).

**Checkpoint**: T005–T010 pass. `mvn install -pl io -am` is green.

---

## Phase 3: User Story 1 - Load and write with a defined schema (Priority: P1) 🎯 MVP

**Goal**: Every source, in every binding, takes a schema set and lands on its schema, and every
sink keeps that schema when it writes.

**Independent Test**: Three NDJSON batches of differing content, each read with a lake's schema
set and appended to Delta, leave the table's schema equal to the set.

### Tests for User Story 1 ⚠️ write first, confirm failing

- [ ] T017 [P] [US1] Test every source with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/DefinedSchemaSourceTest.java`.
      The sources are ndjson, bundles, parquet, delta, tables, bulk (through the existing bulk test
      fixture) and datasets. For each:
    - the exact base schema plus configured annotations, for batches missing schema elements and
      batches carrying extra ones (FR-001, FR-006–FR-008, SC-003);
    - a partial set projects the named types and infers the rest (FR-010);
    - `withSchemas` accepts a JSON string and a map (FR-005);
    - `parquetWithSchemas` is gone (FR-003).
- [ ] T018 [P] [US1] Test writing data loaded with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/sink/DefinedSchemaWriteTest.java`:
    - parquet, delta and tables write each named type with the set's schema (US1 scenario 5,
      SC-003);
    - three Delta appends of differing batches, each loaded with the table's schema set, keep the
      table's schema (SC-002);
    - a Delta merge of data loaded with a narrower schema nulls the missing fields of matched rows
      (edge case);
    - an append of data whose schema differs from the table's widens it (edge case).
- [ ] T019 [P] [US1] Test the Python sources with schema sets given as a dict and as a
      JSON string, including more than ten types, and the refusal of a Spark `StructType` (FR-004),
      in `lib/python/tests/test_defined_schemas.py`.
- [ ] T020 [P] [US1] Test the R sources with schema sets given as a named list and as a
      JSON string, including more than ten types, in `lib/R/tests/testthat/test-defined-schemas.R`.

### Implementation for User Story 1

- [ ] T021 [US1] Add `withSchemas(String)` and `withSchemas(Map<String, String>)`, remove
      `parquetWithSchemas`, and pass the parsed `SchemaSet` to every source constructor, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DataSourceBuilder.java`.
- [ ] T022 [US1] Project each named type after `LayoutDetector.check`, with `Projection` then
      `Annotator`, in `library-api/src/main/java/au/csiro/pathling/library/io/source/FileSource.java`.
      Drop the `.schema(...)` binding from
      `library-api/src/main/java/au/csiro/pathling/library/io/source/ParquetSource.java`.
- [ ] T023 [P] [US1] Project each named type the same way in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/CatalogSource.java` and
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DatasetSource.java`.
- [ ] T024 [P] [US1] Read each named type with the defined-schema reader in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/NdjsonSource.java`,
      `BundlesSource.java` and `BulkDataSource.java` (same directory).
- [ ] T025 [US1] Add the `schemas=` keyword, accepting a dict or a JSON string, to every reader in
      `lib/python/pathling/datasource.py`. Replace the `StructType` form of `parquet(..., schemas=)`.
- [ ] T026 [US1] Add the `schemas` argument, accepting a named list or a JSON string, to every
      reader in `lib/R/R/datasource.R`. Replace `to_java_schema_map` and its `Map.of`
      cap with a JSON string conversion.

**Checkpoint**: T017–T020 pass. US1 independent test passes.

---

## Phase 4: User Story 2 - Encode a stream with a defined schema (Priority: P1)

**Goal**: `encode` and `encodeBundle` take a schema and accept streams. Spark's streaming writer
output is read back by Pathling.

**Independent Test**: A streaming NDJSON source encoded with a schema, written to Delta and
Parquet with `writeStream`, is read back through Pathling.

### Tests for User Story 2 ⚠️ write first, confirm failing

- [ ] T027 [P] [US2] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/StreamingEncodeTest.java`:
    - `encode` of streaming JSON and XML, and `encodeBundle` of streaming bundles, with a schema
      return a streaming dataset with the layout schema (FR-012);
    - without a schema, the encode is refused with the message of research R12 (FR-013);
    - a batch `encode` with a schema gives the schema's output (FR-002).
- [ ] T028 [P] [US2] Test that an encoded stream written with `writeStream` to
      `<path>/Patient.parquet` in Delta and in Parquet format is read back by `DeltaSource` and
      `ParquetSource` with every resource (FR-014, SC-001), in the same file as T027.
- [ ] T029 [P] [US2] Test streaming `encode` and `encode_bundle` with a schema, and the refusal
      without one, in `lib/python/tests/test_encoders.py`.
- [ ] T030 [P] [US2] Test streaming `pathling_encode` and `pathling_encode_bundle` with a schema,
      and the refusal without one, in `lib/R/tests/testthat/test-encoding.R`.

### Implementation for User Story 2

- [ ] T031 [US2] Add the `encode` and `encodeBundle` overloads with a schema, route them to the
      defined-schema reader, and allow streams only when a schema is given, with the new refusal
      message, in `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`.
- [ ] T032 [US2] Make T028 pass. Research R9 expects no change. If Parquet read-back of a
      streaming sink directory fails, fix file grouping in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/FileSource.java`.
- [ ] T033 [P] [US2] Add the `schema=` keyword to `encode` and `encode_bundle` in
      `lib/python/pathling/context.py`.
- [ ] T034 [P] [US2] Add the `schema` argument to `pathling_encode` and `pathling_encode_bundle` in
      `lib/R/R/encoding.R`.

**Checkpoint**: T027–T030 pass.

---

## Phase 5: User Story 3 - Check a destination before writing (Priority: P2)

**Goal**: A public check, shared with the sinks, refuses a destination that a sink would refuse,
and returns the table paths.

**Independent Test**: The check fails on a legacy-layout table and on the wrong timestamp setting,
and passes on an empty location.

### Tests for User Story 3 ⚠️ write first, confirm failing

- [ ] T035 [P] [US3] Test the following for `delta` and `parquet` in
      `library-api/src/test/java/au/csiro/pathling/library/io/sink/DestinationCheckTest.java`:
    - a legacy-layout table gives the same exception and message as the sink;
    - timestamp-carrying datasets with a non-micros session give the sink's refusal;
    - an empty location and a PoF table pass;
    - the returned paths are `<path>/<type>.parquet`;
    - streaming datasets are accepted (FR-015, FR-016).
- [ ] T036 [P] [US3] Test `check_destination` in `lib/python/tests/test_defined_schemas.py` and
      `pathling_check_destination` in `lib/R/tests/testthat/test-defined-schemas.R`.

### Implementation for User Story 3

- [ ] T037 [US3] Extract the layout and timestamp refusals into
      `library-api/src/main/java/au/csiro/pathling/library/io/sink/DestinationCheck.java`, and call it
      from `DeltaSink.java` and `ParquetSink.java` (same directory). Run `SinkDetectionTest` and
      `RangeAnnotationTypeTest` unchanged.
- [ ] T038 [US3] Add `checkDestination(format, path, datasets)` in
      `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`.
- [ ] T039 [P] [US3] Add `check_destination` in `lib/python/pathling/context.py`, and
      `pathling_check_destination` in `lib/R/R/context.R`.

**Checkpoint**: T035–T036 pass.

---

## Phase 6: User Story 4 - Take a schema set from existing data (Priority: P2)

**Goal**: Every source returns its schema set, and the set round-trips through JSON into every
source.

**Independent Test**: For every source, `schemas()` round-tripped through a file is accepted by
every source.

### Tests for User Story 4 ⚠️ write first, confirm failing

- [ ] T040 [P] [US4] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/SourceSchemasTest.java`:
    - `schemas()` on every source has one entry for each type held (FR-017);
    - a Parquet source with differing files returns the merged schema (FR-018);
    - a set written to a JSON file and read back is accepted by every source (FR-019,
      SC-007);
    - a single entry is accepted by `encode` and `encodeBundle` (FR-005).
- [ ] T041 [P] [US4] Test `DataSource.schemas()` round trips through `json.dump`/`json.load` in
      `lib/python/tests/test_defined_schemas.py`, and `ds_schemas()` through `jsonlite` in
      `lib/R/tests/testthat/test-defined-schemas.R`.

### Implementation for User Story 4

- [ ] T042 [US4] Add `schemas()` to
      `library-api/src/main/java/au/csiro/pathling/library/io/source/QueryableDataSource.java`, and
      implement it once in `AbstractSource.java` (same directory).
- [ ] T043 [P] [US4] Add `DataSource.schemas()` in `lib/python/pathling/datasource.py` and
      `ds_schemas()` in `lib/R/R/datasource.R`.

**Checkpoint**: T040–T041 pass.

---

## Phase 7: Engine parity (FR-021, FR-021b)

**Purpose**: Show that query results do not depend on which subset of the uber-schema is stored
(ADR-0007).

- [ ] T044 Map every position kind of SC-005 to the existing absence tests named in the survey,
      and record the matrix and its gaps in `specs/2881_defined-schemas/evidence/absence-coverage.md`.
- [ ] T045 Add failing-first tests for each gap found by T044 in
      `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`. The gaps include:
    - a repeating complex element absent on both sides of a combination;
    - a primitive metadata group absent inside a repeating complex element.

    Fix any engine defect they expose.

- [ ] T046 [P] Add the widening schema set, inferred from the FHIR specification examples, with a
      note of how it was produced, in `fhirpath/src/test/resources/defined-schemas/widening.json`.
- [ ] T047 Add a `defined` mode in
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/TestSchemaMode.java`. In that mode, load
      each case with its inferred schema merged by name with `widening.json` as a defined schema, in
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/PofDatasets.java`.
- [ ] T048 Add the opt-in `definedSchemaParity` profile to `fhirpath/pom.xml`. It runs the YAML
      FHIRPath suites and `FhirViewShareableComplianceTest` with `pathling.testSchemaMode=defined`, and
      stays out of the default build and the release workflows.
- [ ] T049 Run `mvn test -pl fhirpath -PdefinedSchemaParity` and compare it with the inferred run.
      Record the result in `specs/2881_defined-schemas/evidence/parity-run.md` (SC-005). Fix any
      difference as an engine defect.

---

## Phase 8: Polish & cross-cutting concerns

- [ ] T050 [P] Rewrite the schema documentation for defined schemas in every binding in
      `site/docs/libraries/io/schema.md`:
    - getting, saving and passing a schema set;
    - silent loss of content outside the schema, and wide schemas as the remedy;
    - annotations follow the configuration, and metadata follows the schema;
    - Delta merge narrowing;
    - output equals the given schema only for a PoF schema;
    - bootstrapping a set from a sample (FR-022, FR-023).

    Replace the "streaming dataset cannot be encoded" paragraph and the "fitted to the data" section.

- [ ] T051 [P] Document the `schemas` option on sources, and that sinks keep the source's schema,
      in
      `site/docs/libraries/io/index.md`.
- [ ] T052 [P] Rewrite `site/docs/libraries/examples/streaming.md` with a streaming encode with a
      schema, `check_destination`, and `writeStream` to Delta and to Parquet, in each binding. State
      what such a write skips compared with a Pathling sink (FR-024).
- [ ] T053 Run the streaming example of T052 against a file-based stream and record that it runs
      in `specs/2881_defined-schemas/evidence/streaming-example.md` (SC-008).
- [ ] T054 [P] File GitHub issues for the schema generator (including the 10.x shaping options as
      shortcuts), a schema validator, and Delta idempotent writes, with the PoF_line label. Record
      their numbers under Out of Scope in `specs/2881_defined-schemas/spec.md` (FR-025).
- [ ] T055 [P] Point 001's dense-mode tickets, and its FR-009, FR-010, FR-017, FR-043 streaming
      clause and FR-044, at this specification or the generator issue (FR-026). The files are
      `specs/001-parquet-on-fhir/issues/18-dense-mode.md`,
      `specs/001-parquet-on-fhir/issues/19-dense-parity.md` and `specs/001-parquet-on-fhir/spec.md`.
- [ ] T056 Run `mvn install -pl lib/python,lib/R -am` through `scripts/mvn-isolated.sh`, so that
      every existing test passing no schema passes unchanged (FR-020, SC-006).
- [ ] T057 Run the scenarios of `specs/2881_defined-schemas/quickstart.md`.

---

## Dependencies & Execution Order

- **Setup (T001–T004)**: T001 has no dependencies. T002 and T003 feed T007, T014 and T015. T004
  may add tasks to US1.
- **Foundational (T005–T016)**: depends on T002 and T003. Blocks every story.
    - Tests T005–T010 run in parallel.
    - T011 and T012 run in parallel.
    - T013 follows T012.
    - T014 follows T011.
    - T015 and T016 follow T014.
- **US1 (T017–T026)**: depends on Foundational.
    - T021 precedes T022–T024.
    - T025 and T026 follow T021.
- **US2 (T027–T034)**: depends on Foundational. It needs US1's `DeltaSource` and `ParquetSource`
  only for T028's read-back, which works without a schema set. T033 and T034 follow T031.
- **US3 (T035–T039)**: depends on Foundational only. T038 follows T037, and T039 follows T038.
- **US4 (T040–T043)**: T040's round trip into sources depends on US1 (T021).
  `schemas()` itself (T042) does not.
- **Engine parity (T044–T049)**: T044–T046 can start any time. T047 depends on T015. T048 depends
  on T047, and T049 on T048.
- **Polish (T050–T057)**: the docs follow the APIs they describe (T050 and T051 after US1 and US4,
  T052 after US2 and US3). T053 follows T052. T056 and T057 come last.

Within each phase, tests are written and seen to fail before the implementation. Tasks on the same
file run sequentially.

## Implementation Strategy

MVP is Setup, then Foundational, then US1: defined schemas on every source. It fixes
downstream streaming and external consumers on its own. Then come US2 (streaming encode), US3
(destination check, needed by US2's documentation), US4, engine parity, and polish.
