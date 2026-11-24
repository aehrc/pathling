# Tasks: Non-conformant data reads as empty

**Input**: Design documents in `specs/002-non-conformant-as-empty/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md.

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a test task that precedes its
implementation task, and those tests must be written to fail first. Every test runs under both
`spark.sql.ansi.enabled` settings, as the harness does today. Changes to YAML conformance and FHIRPath-layer
expectations need owner approval before they are committed.

**Organisation**: Tasks are grouped by user story, and each story can be implemented and tested on its own.
Branch `issue/2884`, one PR per story or small group of stories, into `release/11.0.0`.

## Format: `[ID] [P?] [Story?] Description`

---

## Phase 1: Setup

- [ ] T001 Rescope #2884's body to point at `specs/002-non-conformant-as-empty/spec.md`, and close #2921 with a comment saying it is folded into #2884 (owner approves the text first).
- [ ] T002 Create the shared corpus in `io/src/test/resources/nonconformant/`: `patients.ndjson`, with one resource per row of `data-model.md` levels 2 and 3, and `patients.xml`, with the same content. Expose them, and T003's fixtures, to `fhirpath` tests: add the `test-jar` goal in `io/pom.xml`, the managed test-jar entry in the root `pom.xml`, and a test-scope test-jar dependency in `fhirpath/pom.xml` (today `fhirpath` sees only `io`'s main jar).
- [ ] T003 [P] Add a test-side writer for directly written PoF Parquet cases, with and without annotations, in `io/src/test/java/au/csiro/pathling/io/NonConformantParquetFixtures.java`.

---

## Phase 2: Foundational (the safe parses)

**Purpose**: every parse that ingest or the engine performs returns null on invalid input, whatever the ANSI setting (R1, FR-011, FR-013).

- [ ] T004 [P] Write failing tests: `DateRangeKind.compute` gives null bounds for `2020-02-30`, `garbage` and `T25:00:00Z` instead of throwing, in `fhir-schema/src/test/java/au/csiro/pathling/schema/annotation/kinds/DateRangeKindTest.java`.
- [ ] T005 [P] Write failing tests: the time bounds give null for `25:61:00`, in `fhirpath/src/test/java/au/csiro/pathling/sql/misc/TimeBoundaryTest.java`.
- [ ] T006 [P] Write tests pinning `DecimalNumericKind` as the decimal grammar under both ANSI settings (`abc`, `1.2.3`, `""`, `1e400` null; `" 1.5"`, `"+1"` read), in `fhir-schema/src/test/java/au/csiro/pathling/schema/annotation/kinds/DecimalNumericKindTest.java`.
- [ ] T007 Make `compute`, `start` and `end` use the null-returning parse in `fhir-schema/src/main/java/au/csiro/pathling/schema/annotation/kinds/DateRangeKind.java`, merging the `…OrNull` variants into them if nothing still needs the throwing form.
- [ ] T008 Make the time boundary functions return null on invalid input in `fhirpath/src/main/java/au/csiro/pathling/sql/misc/LowBoundaryForTime.java` and `HighBoundaryForTime.java`.

**Checkpoint**: no parse in the code base throws on stored text.

---

## Phase 3: User Story 1 - Conformant data and documentation (P1) 🎯 MVP

**Goal**: the guarantee for conformant data is pinned and documented (FR-001 to FR-003, FR-018 to FR-020).

### Tests (write first, confirm failing)

- [ ] T009 [P] [US1] Write tests that a `dateTime` and an `instant` with a time but no timezone are stored unchanged and compared as valid values, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "conformant").

### Implementation

- [ ] T010 [US1] Make T009 pass if it does not already, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/comparison/TemporalComparator.java`.
- [ ] T011 [US1] Add the section "Conformant and non-conformant data" to `site/docs/libraries/io/schema.md`: the rule, the closed exceptions (FR-002), level 1 failing the read, no validation, one example per row of `data-model.md`, and the divergences of FR-020. Update the "Round trip" section to match.
- [ ] T012 [US1] Rewrite FR-063 and remove FR-016's exception 3 in `specs/001-parquet-on-fhir/spec.md`. Mark ticket 31 as superseded by this specification in `specs/001-parquet-on-fhir/issues/31-engine-trust-mode.md`, and note the supersession of "Modes" in `specs/001-parquet-on-fhir/evidence/ingress-strictness/README.md`.

**Checkpoint**: US1 complete; the full suite passes over conformant data.

---

## Phase 4: User Story 2 - Non-conformant JSON and XML load (P1)

**Goal**: ingest never fails above level 1 (FR-004 to FR-006, FR-009, FR-010, FR-015).

### Tests (write first, confirm failing)

- [ ] T013 [P] [US2] Write tests, over `nonconformant/patients.ndjson`, that the load succeeds and stores each level 2 and level 3 row of `data-model.md` as listed, in `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java`.
- [ ] T014 [P] [US2] Write tests that `nonconformant/patients.xml` stores the same data as the JSON, in `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java` (section "XML parity").
- [ ] T015 [P] [US2] Write tests that an invalid date or decimal is stored as its text with a null annotation, in `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java` (section "annotations").
- [ ] T016 [P] [US2] Write tests that malformed JSON and XML still fail the read, kept or moved from the existing suites into `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java`.
- [ ] T017 [P] [US2] Write a test that ingest logs nothing about non-conformant content, in `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java`.

### Implementation

- [ ] T018 [US2] Treat `""`, `[]` and a top-level or nested `null` as absent in `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java`.
- [ ] T019 [US2] Plan each element per column: a safe cast where one reaches the declared type, and otherwise a null of the declared type (R3), in `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java` and `io/src/main/java/au/csiro/pathling/io/PrimitiveConverters.java`.
- [ ] T020 [US2] Replace any remaining plain cast with its safe form in `io/src/main/java/au/csiro/pathling/io/PrimitiveConverter.java`.
- [ ] T021 [US2] Apply the same rules to XML in `io/src/main/java/au/csiro/pathling/io/XmlIngestShape.java`.

**Checkpoint**: US2 complete.

---

## Phase 5: User Story 3 - Arrays keep their valid items (P1)

**Goal**: FR-007 and FR-008.

### Tests (write first, confirm failing)

- [ ] T022 [P] [US3] Write tests for the `Annotator.repeatingGroup` cases: more `_given` groups than values, and `given: []` beside groups, in `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java` (section "arrays").
- [ ] T023 [P] [US3] Write tests pinning the engine's positions over ingested and directly written data: `[null, "Ann"]` with no metadata gives `count()` 1 and `first()` `"Ann"`; a null with an extension is kept; a null complex item is not an item, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "arrays").

### Implementation

- [ ] T024 [US3] Pair over the longer of the values and the metadata groups in `io/src/main/java/au/csiro/pathling/io/Annotator.java`.
- [ ] T025 [US3] Fix whatever T023 shows is not already done by `isElement` (R7) in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`.

**Checkpoint**: US3 complete.

---

## Phase 6: User Story 4 - Stored data from any writer never fails a query (P1)

**Goal**: FR-011, FR-012 (first half), FR-016.

### Tests (write first, confirm failing)

- [ ] T026 [P] [US4] Write tests over T003's directly written cases: comparisons, arithmetic, conversion functions and quantity comparison on an invalid date, time, decimal and quantity value succeed and give empty, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "stored values").
- [ ] T027 [P] [US4] Write tests that each of T026's queries gives identical answers with annotations stored and without them, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "annotation parity").

### Implementation

- [ ] T028 [US4] Route every engine comparison through the null-returning bounds in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/comparison/TemporalComparator.java`.
- [ ] T029 [P] [US4] Make the conversion functions use the shared safe parses in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/function/provider/ConversionLogic.java`.
- [ ] T030 [P] [US4] Make the quantity's canonical form null on an invalid value in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/encoding/QuantityEncoding.java`.

**Checkpoint**: US4 complete apart from scenario 4, which waits for OQ-C.

---

## Phase 7: Spikes for the open questions

Each spike records measurements and reproductions under `evidence/`, then the owner's decision in `research.md` and, where it changes a requirement, in `spec.md`.

- [ ] T031 [P] OQ-A: prototype bottom-up structure emptiness for traversed paths, and measure it on the deepest and widest resource types of the benchmark corpus, in `specs/002-non-conformant-as-empty/evidence/oq-a-structure-emptiness.md`.
- [ ] T032 [P] OQ-B: catalogue today's behaviour for each schema departure and disagreeing annotation, and measure the cost of recomputing annotations, in `specs/002-non-conformant-as-empty/evidence/oq-b-parquet-conformance.md`.
- [ ] T033 [P] OQ-C: list the operations that cross the FHIR-to-System boundary, and measure options (b) and (c), in `specs/002-non-conformant-as-empty/evidence/oq-c-invalid-annotated-values.md`.
- [ ] T034 Record the owner's three decisions in `specs/002-non-conformant-as-empty/research.md`, update FR-012, FR-014 and FR-017 in `spec.md`, and add tasks for the chosen designs below.

---

## Phase 8: User Story 4, scenario 4 (after OQ-C)

- [ ] T035 [US4] Write failing tests for `exists()` and selection on an invalid annotated value, as OQ-C decides, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "OQ-C").
- [ ] T036 [US4] Implement OQ-C's decision in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, plus ingest in `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java` if option (b) is chosen.

---

## Phase 9: User Story 5 - Structures left with no content are absent (P2, after OQ-A)

- [ ] T037 [P] [US5] Write failing tests for US5's scenarios, at the root and nested, singular and repeating, stored directly and ingested, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java` (section "structures") and `io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java` (section "structures").
- [ ] T038 [US5] Implement OQ-A's chosen design in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java` and, where ingest prunes, `io/src/main/java/au/csiro/pathling/io/ResourceTransformer.java`.
- [ ] T039 [US5] Confirm that `NestingDepthRoundTripTest` passes and the cost is within T031's measurement, recorded in `specs/002-non-conformant-as-empty/evidence/oq-a-structure-emptiness.md`.

---

## Phase 10: User Story 6 - Parquet whose schema or annotations do not conform (P3, after OQ-B)

- [ ] T040 [P] [US6] Write failing tests for each schema departure and a disagreeing annotation, as OQ-B decides, in `fhirpath/src/test/java/au/csiro/pathling/test/NonConformantSchemaTest.java`.
- [ ] T041 [US6] Implement OQ-B's decision at planning in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java` and the annotation resolver in `fhir-schema/src/main/java/au/csiro/pathling/schema/annotation/AnnotationResolver.java`.
- [ ] T042 [US6] Document OQ-B's behaviour in `site/docs/libraries/io/schema.md`.

---

## Phase 11: Polish

- [ ] T043 Re-enable the cases excluded for `quantity-example.json` (owner approval): the rule "Non-conformant input fixture: answer is not an array" in `fhirpath/src/test/resources/fhirpath-js/config.yaml`, `NON_CONFORMANT_FIXTURES` in `fhirpath/src/test/java/au/csiro/pathling/test/AnnotationFreeSuiteTest.java`, and `RESOURCE_FILES` in `library-api/src/test/java/au/csiro/pathling/library/FhirPathToColumnTest.java`. Keep only exclusions for cases that expect the object read as an array.
- [ ] T044 Remove or rewrite any remaining test that pins `trust`'s undefined behaviour, found by searching for "non-conformant" and "trust" in `io/src/test` and `fhirpath/src/test`.
- [ ] T045 Run the benchmark against the build before this feature, and record the result against SC-005 in `specs/002-non-conformant-as-empty/evidence/benchmark.md`.
- [ ] T046 Run `quickstart.md`'s scenarios, including the Python and R suites through `scripts/mvn-isolated.sh`.

---

## Dependencies & Execution Order

- **Setup (T001 to T003)**: no dependencies.
- **Foundational (T004 to T008)**: after setup; blocks US1, US2 and US4.
- **US1, US2, US3**: after the foundation; can run in parallel, but US2 and US3 both edit `ResourceTransformer.java` and `NonConformantIngestTest.java`, so their tasks touching those files run in sequence.
- **US4 (T026 to T030)**: after the foundation; independent of US2 and US3 apart from the shared test class.
- **Spikes (T031 to T033)**: can start after setup, in parallel with the stories. T034 waits for all three and the owner.
- **T035 and T036** wait for OQ-C, **US5** for OQ-A, and **US6** for OQ-B.
- **Polish**: after the stories it covers. T043 after US2.

Within each story, the tests are written and seen failing before the implementation.

## Requirement traceability

| Requirement | Tasks                          |
| ----------- | ------------------------------ |
| FR-001      | T009, T046                     |
| FR-002      | T011                           |
| FR-003      | T009, T010                     |
| FR-004      | T013, T018, T021               |
| FR-005      | T013, T019, T020               |
| FR-006      | T013, T019                     |
| FR-007      | T023, T025                     |
| FR-008      | T022, T024                     |
| FR-009      | T013, T020                     |
| FR-010      | T004, T007, T015               |
| FR-011      | T005, T008, T026, T028 to T030 |
| FR-012      | T027, T035, T036               |
| FR-013      | T004, T006, T007               |
| FR-014      | T031, T037, T038               |
| FR-015      | T017, T043, T044               |
| FR-016      | T003, T026                     |
| FR-017      | T032, T040 to T042             |
| FR-018      | T012                           |
| FR-019      | T011                           |
| FR-020      | T006, T011                     |

## Implementation Strategy

MVP is the foundation plus US1, which removes every parse that throws and documents the rule. US2, US3 and US4 follow. The spikes run beside them, so the stories they gate (US5, US6 and US4 scenario 4) are ready to build when the owner decides.
