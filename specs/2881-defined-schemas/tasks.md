# Tasks: Load FHIR resources with a schema defined up front

**Input**: Design documents in `specs/2881-defined-schemas/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a test task that
precedes its implementation task, and those tests must be written to fail first.

**Organisation**: Tasks are grouped by user story so each story can be implemented and tested
independently.

## Delivery: gates and slices

The work is delivered as slices, each built from the tasks below and merged by a pull request into
`issue/2881`. A final pull request takes `issue/2881` to `main`.

| Gate   | Status      | Date       | Note                                      |
| ------ | ----------- | ---------- | ----------------------------------------- |
| A      | approved    | 2026-11-26 | Intent and design                         |
| B      | approved    | 2026-11-26 | Decimal read as text, equal in value      |
| 01 G3  | approved    | 2026-11-26 | #3007, merged                             |
| 02a G3 | approved    | 2026-11-26 | #3008, merged; split from 02 for size     |
| 02b G3 | approved    | 2026-11-26 | #3009, merged                             |
| 03 G2  | approved    | 2026-11-26 | with review fixes; split into 03a/03b/03c |
| 03 G3  | not started |            |                                           |
| 04 G2  | not started |            |                                           |
| 04 G3  | not started |            |                                           |
| 05 G2  | not started |            |                                           |
| 05 G3  | not started |            |                                           |
| 06 G2  | not started |            |                                           |
| 06 G3  | not started |            |                                           |
| 07 G2  | not started |            |                                           |
| 07 G3  | not started |            |                                           |
| Final  | not started |            | `issue/2881` to `main`                    |

G2 asks the owner whether the failing tests say what should happen, before the implementation. G3
asks whether the pull request should merge.

| #   | Slice                              | Tasks                                                             | Blocked by                | G2  |
| --- | ---------------------------------- | ----------------------------------------------------------------- | ------------------------- | --- |
| 00  | Spikes                             | T002, T003                                                        | —                         | no  |
| 01  | Extract `Projection`               | T006, T013, T014                                                  | —                         | no  |
| 02a | `io` foundation                    | T001, T005, T007–T010, T012, T015–T017                            | 00, 01, gate B            | no  |
| 02b | Schema-equality helpers            | T011, T018                                                        | 02a                       | no  |
| 03  | US1: sources take a schema set     | T019–T028, T067, T069, T049, T050                                 | 02                        | yes |
| 04  | US4: schema sets from data         | T037–T040, T057–T066                                              | 02; T037 and T038 need 03 | yes |
| 05  | US2: streaming encode              | T029, T031–T033, T035, T036, T051, T052                           | 02                        | yes |
| 06  | Engine parity and removing `dense` | T041–T048, T054, T068                                             | 02; T043 runs first       | yes |
| 07  | #2884 checks                       | The #2884 parts of T007, T008, T015, T019, T024, T025, T029, T049 | #2884, 03, 05             | yes |
| —   | Close-out                          | T053, T055, T056                                                  | all                       | —   |

Slice 00 has no pull request: its evidence is judged at gate B. T043, the baseline that slice 06 is
compared against, may run before gate B, as may slice 01.

#2884 is far off, so nothing waits for it except slice 07 (decided by the owner on 2026-11-26).
Every part of a task that reuses the check of a schema against the uber-schema that #2884 adds is
moved to slice 07, and the slice that owns the rest of the task leaves it out:

- the refusal of a schema that is not a PoF schema (FR-006b): the refusal cases of T007, T008,
  T019 and T029, the refusal half of T015, and its line in the T049 docs;
- the check of a stored schema under a schema set (FR-006c): the stored-clash case of T019, and
  the check before projection in T024 and T025;
- the part of T008's dropped-content case that compares with a read without a schema, which fails
  by the #2884 check. The drop itself is tested in slice 02.

Until slice 07, a defined schema is not checked against the definitions. A slice before 07 adds no
check of its own in place of #2884's, and leaves no failing or disabled test for the moved
cases. A slice
is split if its pull request would change more than about 800 lines.

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

- [x] T001 Create the evidence index `specs/2881-defined-schemas/evidence/README.md`, listing each
      measurement and the decision it supports.
- [x] T002 [P] Measure whether each step of a defined read needs an action, or fails, over a
      streaming dataset. Record a runnable reproduction and the result of each in
      `specs/2881-defined-schemas/evidence/stream-safety.md` (research R4, R5):
    - `spark.read().json(Dataset<String>)` with a given schema;
    - `from_json` over a streaming dataset;
    - the bundle splitter UDF (`BundleResource.resources` with `FhirJsonReader`'s and
      `FhirXmlReader`'s `SPLIT`) followed by `explode`, over a streaming dataset;
    - `from_xml` over a streaming dataset.
- [x] T003 [P] Confirm which Spark type the inferred JSON read, with `FhirJsonReader`'s options,
      gives a FHIR decimal. Record the reproduction and the result in
      `specs/2881-defined-schemas/evidence/decimal-read.md`, and name that type in
      `specs/2881-defined-schemas/research.md` R3. The type is data-dependent, so the owner fixed
      the defined read's type at gate B: `string`, with a decimal's parity held in value (R3).
- [x] T004 [P] Confirm with the Pathling owner how a resource type that the schema set names but
      the source lacks should behave. Decided: today's behaviour is kept, and an empty table is the
      follow-up #2997. No tasks are added.

---

## Phase 2: Foundational (blocking prerequisites)

**Purpose**: The schema set, the projector, and the defined-schema read in `io`, which every story
uses.

### Tests ⚠️ write first, confirm failing

- [x] T005 [P] Test schema-set parsing in `io/…/io/SchemaSetTest.java`:
    - building from a map of `StructType`, and parsing a schema set file's content (data-model.md);
    - an unknown resource type key is refused with an error naming the key;
    - in parsed content, a value that is not a struct, or is malformed, is refused with an error
      naming the type;
    - an empty set equals no set.
- [x] T006 [P] Test the by-name projector in `io/…/io/ProjectionTest.java`:
    - extra fields are dropped and absent elements are added as typed nulls, at the top level and
      inside structs, arrays and maps;
    - a null struct stays null;
    - field order follows the target.

    A leaf type clash is not tested here: it never reaches the projector, because a stored
    source checks its stored schema first (FR-006c, T019).

- [x] T007 [P] Test observed-shape and layout-schema derivation in
      `io/…/io/DefinedSchemaLogicTest.java`:
    - each primitive kind maps to its ingest type, with decimal as fixed by T003;
    - metadata groups, choice types, and repeating and singular complex elements are mapped;
    - annotation fields are skipped;
    - a schema that is not a PoF schema is refused by the check #2884 adds, with an error naming the
      resource type and path, with no Spark job (FR-006b): a field the definitions do not
      describe, at the top level and nested; a field whose type contradicts them; and a field
      whose cardinality contradicts them, a singular element given as an array and the reverse;
    - annotation fields that the definitions do not describe are not refused (FR-006b, FR-007);
    - the layout schema equals the base schema for a PoF schema (FR-008), including one whose
      fields are out of canonical order at the top level and inside a struct, an array of structs
      and a primitive metadata group, which keeps that order (SC-009);
    - with annotations enabled, they go last in their struct, and a metadata group holding only
      annotations goes right after its primitive, whatever the given order (FR-008);
    - a given schema with non-nullable fields and field metadata yields every field nullable and
      none of the given metadata, as an inferred load does (FR-008).
- [x] T008 [P] Test defined-schema reads in `io/…/io/DefinedSchemaReadTest.java`, for JSON files,
      JSON documents, JSON bundles, XML documents and XML bundles:
    - elements the batch lacks are present and null;
    - content the schema lacks is dropped with no error (FR-006), including content the
      definitions do not describe, which a read of the same data without a schema fails on
      by the check #2884 adds;
    - a schema that is not a PoF schema fails the read with an error naming the resource type and
      path (FR-006b);
    - annotations follow the configuration, whatever annotations the schema carries (FR-007);
    - metadata groups follow the schema whatever the primitive metadata option (edge case);
    - a read without a schema is unchanged (FR-020).
- [x] T009 [P] Test path parity in `io/…/io/DefinedSchemaParityTest.java`:
    - the same resources as JSON, as XML, and as stored PoF Parquet projected through `Projection`
      and `Annotator`, with the same schema, give equal data, a decimal equal in value (FR-009,
      SC-004);
    - a defined load whose schema equals the inferred schema gives the inferred load, a decimal
      equal in value (FR-021a, R3);
    - JSON bundles and XML bundles read with and without a schema give equal data, including
      `urn:uuid` references between entries resolved by `BundleResource.resolve` (research R4).
- [x] T010 [P] Test that the defined reads of a streaming dataset (a `MemoryStream` of strings)
      return a streaming dataset with the layout schema, in
      `io/…/io/DefinedSchemaStreamingReadTest.java`, for JSON documents, XML documents, JSON
      bundles and XML bundles.
- [x] T011 [P] Test the schema-equality helpers of spec SC-002, one per binding, in
      `library-api/src/test/java/au/csiro/pathling/library/io/SchemaEqualityTest.java`,
      `lib/python/tests/test_schema_equality.py` and
      `lib/R/tests/testthat/test-schema-equality.R`:
    - schemas that differ only in nullability (of fields, array elements and map values) or in
      field metadata, at the top level and nested, are the same;
    - schemas that differ only in annotation fields are the same;
    - a missing, extra, renamed or retyped field, or a different field order, makes them differ;
    - two schema sets are the same when they name the same types and each pair is the same.

### Implementation

- [x] T012 [P] Implement the internal `SchemaSet` (`parse(String)`, `of(Map<String, StructType>)`,
      lookup by type, validation against the definitions) in
      `io/src/main/java/au/csiro/pathling/io/SchemaSet.java` (research R10).
- [x] T013 [P] Implement `Projection.project(Dataset<Row>, StructType)` in
      `io/src/main/java/au/csiro/pathling/io/Projection.java`, extracted from
      `DeltaSink.conformToTarget`/`conformColumn`/`conformFields`.
- [x] T014 Replace `DeltaSink`'s private conform methods with `Projection` in
      `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`. Run
      `NarrowMergeTest` and `DeltaSchemaMergingTest` unchanged.
- [x] T015 Refuse a defined schema that is not a PoF schema with the check of a schema against
      the uber-schema that #2884 adds, ignoring its
      annotation fields (FR-006b), and implement the observed-shape derivation, skipping
      annotation fields, in
      `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java`. On a defined-schema read
      the walk keeps or drops each primitive metadata group as the schema says and does not
      consult `storePrimitiveMetadata` (today checked in `ResourceTransformer`'s primitive and
      repeating-element paths), so that T008's metadata case passes. Implement `observedShape(type,
schema)` and `layoutSchema(type, schema)` in
      `io/src/main/java/au/csiro/pathling/io/DefinedSchemaLogic.java`.
- [x] T016 Add defined-schema overloads of `read(type, path)`, `read(type, documents)` and
      `readBundles(type, bundles)`:
    - declare them in `io/src/main/java/au/csiro/pathling/io/FhirFormatReader.java`;
    - implement them in `io/src/main/java/au/csiro/pathling/io/FhirJsonReader.java` with
      `from_json`, or `spark.read().schema(...)` for files, and no inference (research R4);
    - the bundle overload keeps the splitter UDF, the `explode` and the reference resolution of
      `readBundles`, passing `reference -> BundleResource.resolve(reference, references)` to
      `toLayout`, and drops only the inference read.
- [x] T017 Implement the XML overloads in `io/src/main/java/au/csiro/pathling/io/FhirXmlReader.java`,
      building the parse schema from the observed shape in
      `io/src/main/java/au/csiro/pathling/io/XmlIngestShape.java` and skipping the inference read
      (research R5). The bundle overload keeps the splitter UDF, the references column and the
      `BundleResource.resolve` call of the private `toLayout`, as the inferred bundle read does.
- [x] T018 [P] Implement the schema-equality helpers that T011 tests:
      `library-api/src/test/java/au/csiro/pathling/library/io/SchemaEquality.java` (removing
      annotation fields with `AnnotationNames.isAnnotation`),
      `lib/python/tests/schema_equality.py` (`same_schema`, `same_schema_set`, over `StructType`)
      and `lib/R/tests/testthat/helper_schema_equality.R`, which testthat loads before every test
      file.

**Checkpoint**: T005–T011 pass. `mvn install -pl library-api -am` is green, which runs T005–T010
and the Java part of T011, and `scripts/mvn-isolated.sh install -pl lib/python,lib/R -am` runs its
Python and R parts.

---

## Phase 3: User Story 1 - Load and write with a defined schema (Priority: P1) 🎯 MVP

**Goal**: Every source, in every binding, takes a schema set and lands on its schema, and every
sink keeps that schema when it writes.

**Independent Test**: Three NDJSON batches of differing content, each read with a lake's schema
set and appended to Delta, leave the table's schema equal to the set.

### Tests for User Story 1 ⚠️ write first, confirm failing

- [x] T019 [P] [US1] Test every source with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/DefinedSchemaSourceTest.java`.
      The sources are ndjson, bundles, parquet, delta, tables, bulk (through the existing bulk test
      fixture) and datasets. For each:
    - the exact base schema, compared with `SchemaEquality`, plus the configured annotations,
      for batches missing schema elements and batches carrying extra ones (FR-001,
      FR-006–FR-008, SC-003);
    - a partial set projects the named types and infers the rest (FR-010);
    - a schema that is not a PoF schema fails the read with an error naming the resource type and
      path (FR-006b);
    - for parquet, delta, tables and datasets, a stored type that clashes with the definitions,
      such as `Patient.birthDate` stored as `int`, fails the read with a schema set as it does
      without one, by the check #2884 adds, with an error naming the resource type and path
      (FR-006c). This assumes #2884 fails such a read; if its OQ-B decides otherwise, the case
      follows it;
    - for ndjson and bundles (JSON and XML), the dataset returned for a named type is shaped once:
      it has the same analysed plan (`sameResult`) as the `io` defined read of T016 or T017 on
      the same input with the same schema, with no `Projection` or `Annotator` step on top;
    - `withSchemas` accepts a `Map<String, StructType>`, including more than ten types (FR-004,
      FR-005);
    - `parquetWithSchemas` is gone (FR-003).
- [x] T020 [P] [US1] Test writing data loaded with a schema set in
      `library-api/src/test/java/au/csiro/pathling/library/io/sink/DefinedSchemaWriteTest.java`:
    - parquet, delta and tables write each named type with the set's schema, compared with
      `SchemaEquality` (US1 scenario 5, SC-003);
    - three Delta appends of differing batches, each loaded with the table's schema set under the
      annotation configuration the table was written with, keep the table's schema by
      `SchemaEquality` (SC-002);
    - a Delta merge of data loaded with a narrower schema nulls the absent elements of matched rows
      (edge case);
    - an append of data whose schema differs from the table's widens it (edge case).
- [x] T021 [P] [US1] Test the Python sources with schema sets given as a `dict[str, StructType]`,
      including more than ten types, and the refusal of a value that is not a `StructType`, such
      as a dict or a JSON string, with an error naming its key (FR-004), in
      `lib/python/tests/test_defined_schemas.py`. Compare output schemas with
      `same_schema_set` from `lib/python/tests/schema_equality.py`.
- [x] T022 [P] [US1] Test the R sources with schema sets given as a named list of strings, each a
      struct type's JSON form, including more than ten types, and the refusal of a string that is
      not a struct type's JSON form with an error naming its key (FR-004), in
      `lib/R/tests/testthat/test-defined-schemas.R`.
      Compare output schemas with the helper of `lib/R/tests/testthat/helper_schema_equality.R`.
- [x] T067 [P] [US1] Test the debug log of dropped paths in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/DroppedPathLogTest.java`, with
      a Logback `ListAppender` on the sources' logger, as `TraceExpressionTest` captures its log:
      parquet, delta, tables and datasets read with a schema set narrower than the stored schema
      log each dropped path, other than annotations, at debug level; no Spark job runs to find them;
      and ndjson and bundles read with a schema set log no dropped path (FR-006a).

### Implementation for User Story 1

- [x] T023 [US1] Add `withSchemas(Map<String, StructType>)`, remove `parquetWithSchemas`, and pass
      the `SchemaSet` built with `SchemaSet.of` to every source constructor, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DataSourceBuilder.java`.
      Migrate the three `parquetWithSchemas` tests of
      `library-api/src/test/java/au/csiro/pathling/library/io/source/SchemaMergingTest.java`
      (:147, :169, :185) to `withSchemas`: a narrower schema is now projected to, not used as
      given, so their expectations follow the projection (FR-003, FR-006).
- [x] T024 [US1] Project each named type, with `Projection` then `Annotator`, in the transformer
      function that `ParquetSource` and `DeltaSource` pass to the `FileSource` constructor, today
      the identity `(sourceData, resourceType) -> sourceData`, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/ParquetSource.java` and
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DeltaSource.java`.
      `FileSource.load` applies it after `LayoutDetector.check` and is not changed, since
      `NdjsonSource` and `BundlesSource` share it (research R6). The stored schema passes the check
      #2884 adds first, as in a read without a schema; a schema set does not skip it (FR-006c). Drop the `.schema(...)` binding
      from `ParquetSource.readerFor`. Log at debug level the paths of the stored schema that the
      defined schema drops, found by comparing the two schemas only, with no Spark job and no
      per-row work (FR-006a), as T067 tests.
- [x] T025 [P] [US1] Project each named type the same way, after the check of the stored schema
      that #2884 adds (FR-006c), logging the dropped paths at debug level as T024 does (FR-006a): in `CatalogSource.read`, after `LayoutDetector.check` and
      before `universalOperator`, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/CatalogSource.java`; and in
      `DatasetSource.dataset(type, df)`, where a dataset is registered, in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/DatasetSource.java`. Not in
      `DatasetSource.read`, which `FileSource` inherits, so that NDJSON and bundle reads are not
      shaped twice.
- [x] T026 [P] [US1] Read each named type with the defined-schema reader in
      `library-api/src/main/java/au/csiro/pathling/library/io/source/NdjsonSource.java`,
      `BundlesSource.java` and `BulkDataSource.java` (same directory). These reads do not detect
      or log dropped content (FR-006a).
- [x] T027 [US1] Add the `schemas=` keyword, accepting a `dict[str, StructType]`, to every reader in
      `lib/python/pathling/datasource.py`, refusing any other value. `parquet(..., schemas=)`
      already takes this form; it now applies to every reader, and its separate path to
      `parquetWithSchemas` is replaced by `withSchemas`.
      Place `schemas` before `layout_detection` on `parquet`, `delta` and `tables`, as in
      `contracts/python-api.md`. Rewrite the `parquet` docstring, which claims the read uses a
      supplied schema as given and avoids the footer merge, and state in the `tables` docstring
      that `schema` names the catalog database while `schemas` is the schema set. Migrate
      `test_datasource_parquet_with_supplied_schemas` in `lib/python/tests/test_datasource.py` to
      the projection's semantics.
- [x] T028 [US1] Add the `schemas` argument, accepting a named list of strings, each a struct
      type's JSON form, to every reader in `lib/R/R/datasource.R`. Replace `to_java_schema_map` and
      its `Map.of` cap with a conversion that passes the strings to the JVM unchanged (research
      R10), with no limit on their number. Place `schemas` before `layout_detection` on the
      parquet, delta and tables readers, as in `contracts/r-api.md`. Rewrite the documentation of
      the parquet reader's `schemas` argument to match, and state in that of
      `pathling_read_tables` that `schema` names the catalog database while `schemas` is the schema
      set. Migrate the test "datasource parquet with supplied schemas" in
      `lib/R/tests/testthat/test-datasource.R` (:156) to the projection's semantics.

**Checkpoint**: T019–T022 pass. US1 independent test passes.

---

## Phase 4: User Story 2 - Encode a stream with a defined schema (Priority: P1)

**Goal**: `encode` and `encodeBundle` take a schema and accept streams. Encoded streams written to
Delta and Parquet through a Pathling sink in `foreachBatch` are read back by Pathling.

**Independent Test**: A streaming NDJSON source encoded with a schema, written to Delta and
Parquet with a Pathling sink in `foreachBatch`, is read back through Pathling (T029).

### Tests for User Story 2 ⚠️ write first, confirm failing

- [ ] T029 [P] [US2] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/StreamingEncodeTest.java`:
    - `encode` of streaming JSON and XML, and `encodeBundle` of streaming bundles, with a schema
      return a streaming dataset with the layout schema (FR-012);
    - without a schema, the encode is refused with `UnsupportedOperationException`, as today, and
      the message of research R12, which keeps the `foreachBatch` recommendation (FR-013);
    - a batch `encode` with a schema gives the schema's output (FR-002);
    - `encode` and `encodeBundle` with a schema that is not a PoF schema fail with an error naming
      the resource type and path (FR-006b);
    - a stream encoded with a schema and written through a Pathling sink inside `foreachBatch`, to
      Delta and to Parquet, is read back through Pathling with the schema's output (US2 independent
      test).
- [x] T030 Removed with FR-014: the read-back of a raw `writeStream` is not tested.
- [ ] T031 [P] [US2] Test streaming `encode` and `encode_bundle` with a schema, and the refusal
      without one, in `lib/python/tests/test_encoders.py`, together with batch `encode` and
      `encode_bundle` with a schema, for JSON and XML input (FR-002, FR-012).
- [ ] T032 [P] [US2] Test streaming `pathling_encode` and `pathling_encode_bundle` with a schema,
      and the refusal without one, in `lib/R/tests/testthat/test-encoding.R`, together with batch
      `pathling_encode` and `pathling_encode_bundle` with a schema, for JSON and XML input (FR-002,
      FR-012).

### Implementation for User Story 2

- [ ] T033 [US2] Add the `encode` and `encodeBundle` overloads with a `@Nullable StructType` schema,
      route them to the
      defined-schema reader, and allow streams only when a schema is given, keeping
      `UnsupportedOperationException` with the new refusal message, in
      `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`.
- [x] T034 Removed with T030.
- [ ] T035 [P] [US2] Add the `schema=` keyword, taking a `StructType`, to `encode` and
      `encode_bundle` in
      `lib/python/pathling/context.py`.
- [ ] T036 [P] [US2] Add the `schema` argument, taking a struct type's JSON form as a string, to
      `pathling_encode` and `pathling_encode_bundle` in
      `lib/R/R/encoding.R`.

**Checkpoint**: T029–T032 pass.

---

## Phase 5: User Story 4 - Take a schema set from existing data (Priority: P2)

**Goal**: Every source returns its schema set, and the set round-trips through a schema set file
into every source.

**Independent Test**: For every source, `schemas()` round-tripped through a file is accepted by
every source.

### Tests for User Story 4 ⚠️ write first, confirm failing

- [ ] T037 [P] [US4] Test the following in
      `library-api/src/test/java/au/csiro/pathling/library/io/source/SourceSchemasTest.java`:
    - `schemas()` on every source has one entry for each type held (FR-017);
    - a Parquet source with differing files returns the merged schema (FR-018);
    - a set written to a file with `SchemaSets.toJson` and read back with `SchemaSets.fromJson` is
      accepted by every source, and each source's output schema set is the same as it by
      `SchemaEquality` (FR-019, SC-007);
    - a single entry is accepted by `encode` and `encodeBundle` (FR-005).
- [ ] T057 [P] [US4] Test canonicalisation in `io/…/io/DefinedSchemaLogicTest.java` and
      `library-api/src/test/java/au/csiro/pathling/library/io/source/SourceSchemasTest.java`
      (FR-027, SC-009):
    - a set out of canonical order at every depth canonicalises to an equivalent set in canonical
      order, and a canonical set is returned unchanged;
    - choice types, open choices, metadata groups and annotations take their canonical places;
    - a schema from an evolved Delta table canonicalises to the order an inferred load of FHIR
      JSON gives;
    - a field the definitions do not describe is dropped; a field whose type or cardinality
      contradicts them is refused with an error naming the resource type and path; annotation
      fields are kept, in their canonical places; and a key that is not a resource type code is
      refused with an error naming it;
    - a load with the canonicalised set gives canonical output;
    - no Spark job runs.
- [ ] T060 [P] [US4] Test the schema set file functions in
      `library-api/src/test/java/au/csiro/pathling/library/SchemaSetsTest.java` (FR-028):
    - `toJson` gives one JSON object mapping each resource type code to its `StructType.json()`
      object (data-model.md);
    - `fromJson(toJson(set))` equals the set exactly, field order, nullability and field metadata
      included;
    - malformed content, and a value that is not a struct, are refused with
      `IllegalArgumentException`, the latter naming its key;
    - an empty object gives an empty set.
- [ ] T061 [P] [US4] Test `write_schemas` and `read_schemas` in
      `lib/python/tests/test_schema_files.py` (FR-028):
    - a set written and read back is equal to it;
    - the file written parses to the same JSON object as `SchemaSets.toJson` of the same set, and
      a file written from `SchemaSets.toJson` is read to the same set, so files cross bindings;
    - a malformed file is refused with an error naming the file, and a value that is not a struct
      with an error naming its key;
    - both run without a Spark session.
- [ ] T062 [P] [US4] Test `pathling_write_schemas(sc, schemas, path)` and
      `pathling_read_schemas(sc, path)` in `lib/R/tests/testthat/test-schema-files.R`, with the
      cases of T061 other than the last, and that a schema carrying empty and nested field
      metadata survives a round trip unchanged (FR-028).
- [ ] T066 [P] [US4] Test `pc.canonical_schemas` in `lib/python/tests/test_defined_schemas.py`
      and `pathling_canonical_schemas` in `lib/R/tests/testthat/test-defined-schemas.R`: a set out
      of order canonicalises to the order an inferred load gives (FR-027).
- [ ] T038 [P] [US4] Test that `DataSource.schemas()` round trips through `write_schemas` and
      `read_schemas` into every source in `lib/python/tests/test_defined_schemas.py`, and
      `ds_schemas()` through `pathling_write_schemas` and `pathling_read_schemas` in
      `lib/R/tests/testthat/test-defined-schemas.R`, comparing with each binding's schema-equality
      helper (FR-019, SC-007).

### Implementation for User Story 4

- [ ] T039 [US4] Add `schemas()` to
      `library-api/src/main/java/au/csiro/pathling/library/io/source/QueryableDataSource.java`, and
      implement it once in `AbstractSource.java` (same directory).
- [ ] T040 [P] [US4] Add `DataSource.schemas()`, returning `dict[str, StructType]`, in
      `lib/python/pathling/datasource.py`, and `ds_schemas()`, returning a named list of strings,
      each a struct type's JSON form, in `lib/R/R/datasource.R`.
- [ ] T058 [US4] Implement canonicalisation in `io/…/io/DefinedSchemaLogic.java`, sorting each
      struct by the canonical structure, and expose it in `library-api` as
      `PathlingContext.canonicalSchemas(Map<String, StructType>)`, returning
      `Map<String, StructType>` (FR-027).
- [ ] T059 [P] [US4] Expose canonicalisation as `pc.canonical_schemas(schemas)`, taking and
      returning `dict[str, StructType]`, in Python, and as `pathling_canonical_schemas(pc, schemas)`,
      taking and returning a named list of strings, in R (FR-027).
- [ ] T063 [US4] Implement `SchemaSets.toJson` and `SchemaSets.fromJson`, delegating parsing to
      `SchemaSet.parse`, in `library-api/src/main/java/au/csiro/pathling/library/SchemaSets.java`
      (FR-028, research R7, R10).
- [ ] T064 [P] [US4] Implement `write_schemas` and `read_schemas` in pure Python, over
      `StructType.json()` and `StructType.fromJson`, in `lib/python/pathling/schemas.py`, and
      export them from `lib/python/pathling/__init__.py` (FR-028).
- [ ] T065 [US4] Implement `pathling_write_schemas(sc, schemas, path)` and
      `pathling_read_schemas(sc, path)` in `lib/R/R/schemas.R`, delegating to
      `SchemaSets.toJson` and `SchemaSets.fromJson` through the Spark connection, and export them
      (FR-028, research R7).

**Checkpoint**: T037, T038, T057, T060–T062 and T066 pass.

---

## Phase 6: Engine parity (FR-021, FR-021b)

**Purpose**: Show that query results do not depend on which subset of the uber-schema is stored
(ADR-0007).

- [ ] T041 Map every position kind of SC-005 to the existing absence tests named in the survey,
      and record the matrix and its gaps in `specs/2881-defined-schemas/evidence/absence-coverage.md`.
- [ ] T042 Add failing-first tests for each gap found by T041 in
      `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`. The gaps include:
    - a repeating complex element absent on both sides of a combination;
    - a primitive metadata group absent inside a repeating complex element.

    Fix any engine defect they expose.

- [x] T043 Record the baseline: run `YamlReferenceImplTest` and `YamlFhirPathTest` with today's
      loads, and record each case's result in
      `specs/2881-defined-schemas/evidence/yaml-defined-run.md`, under a baseline heading.
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
      `specs/2881-defined-schemas/evidence/yaml-defined-run.md` (FR-021b, SC-005).

---

## Phase 7: Polish & cross-cutting concerns

- [ ] T049 [P] Rewrite the schema documentation for defined schemas in every binding in
      `site/docs/libraries/io/schema.md`:
    - getting, saving and passing a schema set, with the schema set file functions (FR-028);
    - loss of content outside the schema with no error (logged at debug level only for stored
      sources, FR-006a), and wide schemas as the remedy;
    - annotations follow the configuration, and metadata follows the schema;
    - Delta merge narrowing;
    - a schema that is not a PoF schema is refused;
    - NDJSON, bundle, XML and streaming reads with a schema drop content the schema has no field
      for silently, where a read without a schema fails fast by the check #2884 adds, while stored reads check the
      stored schema either way;
    - output keeps the given field order, canonicalising a set to get canonical order, and a read
      without a schema keeping the stored order;
    - bootstrapping a set from a sample (FR-022, FR-023).

    Replace the "streaming dataset cannot be encoded" paragraph and the "fitted to the data" section.

- [ ] T050 [P] Document the `schemas` option on sources, and that sinks keep the source's schema,
      in `site/docs/libraries/io/index.md`. Replace its Scala and Java `parquetWithSchemas`
      examples (:497-519) with `withSchemas`, and remove the claim that a supplied schema is used
      as given.
- [ ] T051 [P] Rewrite `site/docs/libraries/examples/streaming.md` with a streaming encode with a
      schema written to Delta and to Parquet through a Pathling sink inside `foreachBatch`, for
      example `pc.read.datasets({...}).write.delta(path, save_mode="merge")`, extending the 10.x
      `foreachBatch` example, in Python and Java. State that a write with Spark's own
      `writeStream` skips the checks a Pathling sink makes (FR-024).
- [ ] T052 Run the streaming example of T051 against a file-based stream and record that it runs
      in `specs/2881-defined-schemas/evidence/streaming-example.md` (SC-008).
- [ ] T053 [P] File GitHub issues for the schema generator (including the 10.x shaping options as
      shortcuts) and Delta idempotent writes, with the PoF_line label. Checking a schema against
      the uber-schema is #2884's work, once it is re-scoped, not a follow-up. Record
      their numbers under Out of Scope in `specs/2881-defined-schemas/spec.md` (FR-025).
- [ ] T054 [P] Point 001's dense-mode tickets, and its FR-009 dense schema, FR-010, FR-016
      exception 4, FR-017, FR-043 streaming clause, FR-044, FR-047 schema modes, User Story 4
      scenario 7, the dense subset of SC-002 and SC-010, at this specification or the generator
      issue (FR-026). The files are
      `specs/001-parquet-on-fhir/issues/18-dense-mode.md`,
      `specs/001-parquet-on-fhir/issues/19-dense-parity.md` and `specs/001-parquet-on-fhir/spec.md`.
    - Remove the `dense` placeholder (which refers to M6 and T037b of 001), superseded by this
      specification's defined-schema YAML loads, from
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/TestSchemaMode.java`, with its tests in
      `fhirpath/src/test/java/au/csiro/pathling/test/layout/TestDimensionsTest.java`.
- [ ] T068 Measure the regular build time of `fhirpath` before and after Phase 6, on the same
      machine, and record the growth, with the time the new tests and the derivation of each YAML
      suite's schemas take, in `specs/2881-defined-schemas/evidence/build-time.md` (SC-005).
- [ ] T069 [P] Write the release note that FR-022a requires: a Parquet read with a defined schema
      merges every file's footer, as a read without a schema does, so it loses the saving that the
      former `schemas` option of the Parquet source gave. Put it in the pull request description
      under a release-note heading, from which the release notes are drafted (FR-022a).
- [ ] T055 Run `mvn install -pl lib/python,lib/R -am` through `scripts/mvn-isolated.sh`, so that
      every existing test passing no schema passes unchanged (FR-020, SC-006).
- [ ] T056 Run the scenarios of `specs/2881-defined-schemas/quickstart.md`.

---

## Dependencies & Execution Order

- **Setup (T001–T004)**: T001 has no dependencies. T002 and T003 feed T007 and T015–T017. T004
  is done and adds no tasks.
- **#2884**: the check of a schema against the uber-schema that #2884 adds must land before
  slice 07, and before nothing else; its issue and specification are being re-scoped to include
  it. The refusal of a
  schema that is not a PoF schema (T015, and the refusal cases of T007, T008, T019 and T029) and
  the check of stored schemas under a schema set (T024, T025, and the stored-clash case of T019)
  reuse it, as does the case of T008 that a read without a schema fails on.
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
    - T067 is written and seen to fail before T024 and T025.
- **US2 (T029–T036)**: depends on Foundational. T035 and T036 follow T033.
- **US4 (T037–T040, T057–T065)**: T037's round trip into sources depends on US1 (T023).
  `schemas()` itself (T039) does not. T058 follows T015; T066 is written and seen to fail
  before T059, which follows T058.
    - T060–T062 are written and seen to fail before T063–T065.
    - T063 follows T012, and T037 uses it.
    - T064 does not depend on T063, but the cross-binding cases of T061 need it. T065 follows
      T063, to which it delegates.
    - T038 uses T064 and T065, and also follows T027, T028 and T040.
- **Engine parity (T041–T048)**: T041, T042 and T043 can start any time. T044 and T045 are written
  and seen to fail before T046 and T047, which depend on T016. T048 follows T046 and T047.
- **Polish (T049–T056, T068, T069)**: the docs follow the APIs they describe (T049 and T050 after
  US1 and US4, T051 after US2). T052 follows T051. T054 follows T047. T068 follows T048. T069
  follows T023. T055 and T056 come last.

Within each phase, tests are written and seen to fail before the implementation. Tasks on the same
file run sequentially.

## Implementation Strategy

MVP is Setup, then Foundational, then US1: defined schemas on every source. It fixes
downstream streaming and external consumers on its own. Then come US2 (streaming encode), US4,
engine parity, and polish.
