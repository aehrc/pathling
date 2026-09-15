# Tasks: Parquet on FHIR

**Input**: Design documents in `specs/001-parquet-on-fhir/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/](contracts/)

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a
test task that precedes its implementation task, and those tests must be written
to fail first.

Three categories are exempt and say so where they appear:

- **Pure motion** (T010–T012): the requirement is that behaviour does not
  change, so *the existing suite is the test*. Verification is that no test
  changes other than its imports.
- **Coverage-first** (T020–T027): tests only, by design, landing before the
  conventions they protect.
- **Pins** (T078): tests that pass on their first run, because they record
  a consequence of an accepted design decision rather than drive new behaviour.
  Their purpose is to fail if that consequence ever silently widens. Do not look
  for a way to make them red first.

**Organisation**: Tasks are grouped by user story so each story can be
implemented and tested independently.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: can run in parallel — different files, or independent test methods
  within one test file, and no dependency on an incomplete task.
- **[Story]**: the user story (US1–US8); omitted for Setup, Foundational and Polish.

---

## Phase 1: Setup

**Purpose**: Capture the baseline and create the module scaffolding. No
behaviour changes.

- [ ] T001 Run the JMH suite on unmodified `main` in `benchmark/src/main/java/au/csiro/pathling/benchmark/PathlingBenchmark.java` and record the results in `evidence/baseline.md`, noting the machine, fork count and Spark version. **Must precede every other task**: once modules move, the comparison point is gone.
- [ ] T002 Split the benchmark so encode, decode and query execution are timed separately, and planning time separately from execution time, in `benchmark/src/main/java/au/csiro/pathling/benchmark/PathlingBenchmark.java`. The current benchmarks report one number for an NDJSON-to-view pipeline, which cannot answer driver 1 either way.
- [ ] T003 Re-run T001 against the split benchmark and record the per-phase baseline in `evidence/baseline.md`.
- [ ] T004 Create the `fhir-schema` module with `fhir-schema/pom.xml`, depending on `utilities` and `spark-sql-api` only.
- [ ] T005 Add an enforcer rule to `fhir-schema/pom.xml` banning `org.apache.spark:spark-catalyst_*` and `org.apache.spark:spark-sql_*`, and verify it fails the build when violated deliberately.
- [ ] T006 [P] Create the `io` module with `io/pom.xml`, depending on `fhir-schema` and `spark-sql`.
- [ ] T007 [P] Add a build rule to `io/pom.xml` failing on any `org.apache.spark.sql.catalyst` import, and verify it fails when violated deliberately.
- [ ] T008 Add both modules to `<modules>` in `pom.xml` in build order, and to the dependency list in `library-runtime/pom.xml` so the Python and R libraries ship them.
- [ ] T009 Add `fhir-schema` and `io` as dependencies of `fhirpath` in `fhirpath/pom.xml`. `encoders/pom.xml` is not modified.

**Checkpoint**: Modules exist and are empty; the boundary is enforced; the baseline is captured.

---

## Phase 2: Foundational

**Purpose**: Everything the user stories depend on. Blocks all of them.

### Definition abstraction (pure motion, then widening)

- [ ] T010 Move `fhirpath/src/main/java/au/csiro/pathling/fhirpath/definition/**` (including `defaults/` and `fhir/`) to `fhir-schema/src/main/java/au/csiro/pathling/definition/`. Pure motion — no test task; behaviour is unchanged by construction.
- [ ] T011 Update the referencing files in `fhirpath` main and test to the new package.
- [ ] T012 Verify the motion by reviewing the test diff: it must contain only package declarations and import statements. This is the test for T010–T011.
- [ ] T013 Test child enumeration in `fhir-schema/src/test/java/au/csiro/pathling/definition/NodeDefinitionTest.java` — children of a resource, a backbone element and a complex type, asserting order and completeness against the FHIR definitions, and a choice element appearing once as a choice rather than pre-expanded.
- [ ] T014 Test cardinality in `fhir-schema/src/test/java/au/csiro/pathling/definition/ElementDefinitionTest.java` — singular, repeating, and choice elements.
- [ ] T015 Test that a type code outside the R4 enumeration is representable, and that both implementations agree on enumeration and cardinality for the same resource, in `fhir-schema/src/test/java/au/csiro/pathling/definition/DefinitionContextAgreementTest.java`.
- [ ] T016 Add child enumeration to `fhir-schema/src/main/java/au/csiro/pathling/definition/NodeDefinition.java`, implemented from the children the HAPI-backed implementation already obtains rather than asking a second time.
- [ ] T017 Add cardinality to `fhir-schema/src/main/java/au/csiro/pathling/definition/ElementDefinition.java`.
- [ ] T018 Replace the R4 enumeration in the reported type with a module-local representation in `fhir-schema/src/main/java/au/csiro/pathling/definition/ElementDefinition.java`, mapping the R4 enumeration onto it in the HAPI-backed implementation.
- [ ] T019 Convert the call sites in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/` to the new type, keeping dispatch typed rather than degrading to string comparison. The engine continues to read cardinality from the Spark schema in this phase.

### Coverage that must land before the conventions change (tests only)

- [ ] T020 [P] Add portable JSON fixtures carrying references between resources, in the same form as the existing `viewTests` fixtures, under `fhirpath/src/test/resources/viewTests/`: a reference that resolves, one whose target is absent, one to a resource type not present, a versioned reference, and a repeating reference element with several targets.
- [ ] T021 [P] Add view test cases for `resolve()` returning the referenced resource, yielding empty on an unresolvable reference, on an absent reference and on an absent resource type, and returning all targets of a repeating reference, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T022 [P] Add cases for `resolve()` composed with traversal and with `where()`, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T023 [P] Add a case exercising resource-key production and reference-key production together with the join they feed, in `fhirpath/src/test/resources/viewTests/`. The primitives have coverage today; the join does not.
- [ ] T024 Convert the two tests using direct resource construction in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/ResolveFunctionDslTest.java` to the declarative model builder, and confirm the class no longer references the HAPI resolver factory.
- [ ] T025 Confirm every new case passes against unmodified behaviour. A failure is a defect to raise separately, not to fix here.
- [ ] T026 [P] Add a divergent-schema fixture under `fhirpath/src/test/resources/viewTests/` whose two files deliberately disagree on a leaf of a repeating element, with a view unnesting that element and projecting only that leaf.
- [ ] T027 Assert against the real engine that the divergent-schema fixture returns resources from both files (FR-036). This pins the unnesting constraint; if it fails, unnesting has been reshaped into a leaf-level read and rows are being lost silently. Once T038e lands, extend the fixture so the projected leaf is absent from one file's schema entirely rather than merely null, so the pin exercises tolerant traversal over divergent files and not only the unnesting shape.

### Schema derivation

- [ ] T028 Test schema derivation in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaBuilderTest.java` — types and cardinality taken from definitions, never from data; a repeating element is an array whether one value or many are present.
- [ ] T029 Test pruning in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaPruningTest.java` — a complex element survives only where some descendant leaf is populated, so a field-less structure never arises (FR-011).
- [ ] T030 Test that the dense schema is the pruned derivation with pruning skipped, asserting identical types, cardinality and field order for the branches both carry (FR-010), in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaModeParityTest.java`.
- [ ] T030a Test that every derived structure orders its fields in definition order, and that a pruned structure's field order is a subsequence of the dense one (FR-057), in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaModeParityTest.java`. Field order is part of the type, so this is a correctness assertion rather than a tidiness one.
- [ ] T031 Implement definition-derived schema derivation in `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaBuilder.java`.
- [ ] T032 Implement pruning as a filter over the derivation output in `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaPruner.java`.
- [ ] T033 Add the schema mode, the strictness switch and the per-annotation toggles to the encoding configuration, keeping the nesting, extension and open-type options bounding the dense mode only (FR-044).

### Test infrastructure for the flag day

- [ ] T034 Route the YAML conformance runner's fixture factory through the JSON path with an explicit derived schema, in `fhirpath/src/test/java/au/csiro/pathling/test/yaml/`.
- [ ] T035 Route the SQL-on-FHIR view test pipeline through the JSON path rather than parsing to objects and encoding, in `fhirpath/src/test/java/au/csiro/pathling/views/FhirViewTest.java`. The local fixtures and the submodule fixtures come along unchanged.
- [ ] T036 Route the object-based test data source through the category-C path — construct, serialise to JSON, read with the derived schema — so the fluent builders survive unchanged, in `fhirpath/src/test/java/au/csiro/pathling/test/`.
- [ ] T037 Add the schema mode switch to the test framework, defaulting to pruned, opt-in dense per test, switchable with `-Dpathling.testSchemaMode=dense`.
- [ ] T038 Add a test asserting the active schema mode matches the requested one and failing loudly otherwise. This repository has precedent for test configuration that silently does nothing; a mode switch that fails quietly would leave a green build in a mode nobody is running.

### Tolerant traversal and type reconciliation

Two additions to the query-time expression toolkit, both resolving after the
input schema is known (FR-052 permits extending it; FR-048 and FR-049 are scoped
to definitions and encoding). They are foundational because both US3 and US4
depend on them, so they carry no story tag.

**T038a is the risk gate for the whole approach.** If it cannot be made to pass,
the design falls back to the unbound column representation recorded in R-017,
and that is much cheaper to discover here than after US4 is built on it.

- [ ] T038a Test that the tolerant traversal expression survives the analyzer when constructed over an **unresolved** attribute — that nothing probes `dataType` before the child resolves and forces the replacement early — in `encoders/src/test/scala/au/csiro/pathling/sql/ResolveOrNullTest.scala`. This is the one residual risk of the chosen approach (R-005) and is not testable from PySpark.
- [ ] T038b [P] Test that the expression resolves to a direct field reference where the input structure carries the field, and to a null of the declared fallback type where it does not, in the same file.
- [ ] T038c [P] Test the fallback types against FR-055: a singular primitive takes the definition's type, a repeating primitive an array of it, a singular complex element the bottom type, a repeating complex element an array of the bottom type. Assert that a repeating fallback survives `transform` — bare `void` does not — and that a complex fallback combines with a populated structure, which a concrete minimal structure does not. In the same file.
- [ ] T038d [P] Test that a plan using the expression prunes identically to one written with a direct field reference, by comparing `ReadSchema` from the executed plan (finding 14), in the same file. The rewrite runs before every pruning rule, so this must hold; it is the assertion that a future Spark upgrade has not reordered the optimizer batches.
- [ ] T038e Implement the tolerant traversal expression as a `RuntimeReplaceable` in `encoders/src/main/scala/au/csiro/pathling/sql/ResolveOrNull.scala`, beside the existing query-time expressions in `encoders/src/main/scala/au/csiro/pathling/encoders/Expressions.scala`, and wrap it for use from Java via `ExpressionUtils` as `ColumnFunctions.structProduct` already does. Scala, because `RuntimeReplaceable` is a Scala trait whose tree-node contract runs through `Product`; `encoders` already carries the Scala plugin and this module is where the query-time toolkit lives, so FR-052 covers it and no POM changes. No codegen: `RuntimeReplaceable` supplies `dataType`, `nullable` and a final `eval`.
- [ ] T038f [P] Test the structure merge in `utilities/src/test/java/au/csiro/pathling/utilities/StructureMergeTest.java` — recursive field-wise union under a supplied field ordering, and therefore commutative and associative regardless of merge order (FR-058).
- [ ] T038g Implement the merge in `utilities/src/main/java/au/csiro/pathling/utilities/StructureMerge.java`, as the single implementation serving both reconciliation and the merging of divergent file schemas (FR-058). It lives in `utilities`, not `fhir-schema`, because both callers must reach it and `encoders` must not gain a dependency on `fhir-schema` (T009, FR-051). It is pure structure mechanics over `spark-sql-api` types and takes the field ordering as an argument.
- [ ] T038h [P] Test the canonical field ordering supplied to the merge in `fhir-schema/src/test/java/au/csiro/pathling/schema/CanonicalOrderTest.java` — the definition order for a FHIR type, and that merging two pruned structures under it yields definition order rather than discovery order (FR-057). This is the input the merge cannot derive: two subsequences of a total order do not determine it, since `[id, family]` and `[id, given]` do not say which of `family` and `given` comes first.
- [ ] T038i Implement the canonical ordering supplier in `fhir-schema/src/main/java/au/csiro/pathling/schema/CanonicalOrder.java` (FR-057).
- [ ] T038j [P] Test the reconciliation expression in `encoders/src/test/scala/au/csiro/pathling/sql/MergeCastTest.scala` — two structures of the same FHIR type with different fitted shapes project by name into the merged type and combine; fields a side lacks become null; the result is traversable. Assert it is a by-name projection and **not** a cast, by constructing two structures with the same field types in a different order and asserting they are not silently transposed.
- [ ] T038k [P] Test that folding the binary form gives the same type and values as one variadic call (finding 16), in the same file.
- [ ] T038l Implement the reconciliation expression as a variadic `RuntimeReplaceable` in `encoders/src/main/scala/au/csiro/pathling/sql/MergeCast.scala`, taking the operand being projected, the full ordered operand list so that every operand computes the same target type, and the canonical field ordering from T038i, which it cannot recover from the operands.

**Checkpoint**: Definitions can drive schema derivation; the join has portable coverage; the unnesting constraint is pinned; fixtures reach Spark without the old encoder; the toolkit can traverse tolerantly and reconcile shapes, and the analyzer risk is resolved either way.

---

## Phase 3: User Story 7 - Detection of earlier layouts (Priority: P2) 🎯 land first

**Goal**: Reading data written by an earlier release fails with an actionable
message rather than producing wrong answers.

**Independent Test**: Read previous-layout data through each file-based source
and assert the error.

*Sequenced first despite its priority: it is correct against the codebase as it
stands, depends on nothing else, and landing it early means the error exists
before there is anything for it to catch.*

### Tests ⚠️ write first, confirm failing

- [ ] T039 [P] [US7] Test the layout detector against a full previous-layout schema, a sparse previous-layout schema, a new-layout schema and a schema with no markers, in `library-api/src/test/java/au/csiro/pathling/library/io/source/LayoutDetectorTest.java`.
- [ ] T040 [P] [US7] Test that detection walks nested structures and arrays of structures, not only top-level fields, in the same file.
- [ ] T041 [P] [US7] Test that reading conforming data succeeds unchanged, that a sparse but conforming schema succeeds, and that an unclassifiable schema succeeds, in `library-api/src/test/java/au/csiro/pathling/library/io/source/FileSourceDetectionTest.java`.
- [ ] T042 [P] [US7] Test that an unsupported layout fails at read time with a message naming the resource type, the detected layout, the expected layout and the remedy, asserting the message content so it cannot silently degrade, in the same file.
- [ ] T043 [P] [US7] Test that the opt-out permits the read and is scoped to one source, in the same file.
- [ ] T044 [P] [US7] Test that detection introduces no additional file access beyond the schema metadata the read already obtains (FR-038), in the same file.

### Implementation

- [ ] T045 [US7] Implement the layout detector, classifying a schema by marker fields and returning the markers found so the message can cite evidence, in `library-api/src/main/java/au/csiro/pathling/library/io/source/LayoutDetector.java`.
- [ ] T046 [US7] Apply the check in `library-api/src/main/java/au/csiro/pathling/library/io/source/FileSource.java`, so `ParquetSource` and `DeltaSource` inherit it.
- [ ] T047 [US7] Apply the check per table as each resource type is resolved in `library-api/src/main/java/au/csiro/pathling/library/io/source/CatalogSource.java`.
- [ ] T048 [US7] Thread the opt-out through the source constructors in `library-api/src/main/java/au/csiro/pathling/library/io/source/`.
- [ ] T049 [P] [US7] Expose the opt-out in `lib/python/pathling/datasource.py` and `lib/R/R/datasource.R`.

**Checkpoint**: Old data is rejected with an actionable message everywhere it can enter.

---

## Phase 4: User Story 1 - The storage layout (Priority: P1)

**Goal**: FHIR JSON is stored in the Parquet on FHIR layout.

**Independent Test**: Load a corpus and inspect the stored schema and values
against the contract, with no FHIRPath evaluation.

### Tests ⚠️ write first, confirm failing

- [ ] T050 [P] [US1] Test that a decimal is stored as its source lexical form with a numeric annotation beside it, in `io/src/test/java/au/csiro/pathling/io/transform/DecimalTransformTest.java`.
- [ ] T051 [P] [US1] Test that a primitive element's id and extensions are stored in the metadata group beside it, in `io/src/test/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransformTest.java`.
- [ ] T052 [P] [US1] Test that extensions on complex elements are stored inline, and that no field identifier and no root-level extension map are emitted, in `io/src/test/java/au/csiro/pathling/io/transform/ExtensionTransformTest.java`.
- [ ] T053 [P] [US1] Test that dates carry range annotations reflecting the stated precision, in `io/src/test/java/au/csiro/pathling/io/annotation/DateRangeAnnotationTest.java`.
- [ ] T054 [P] [US1] Test that a quantity carries a canonical annotation whose precision preserves magnitude, asserting that quantities differing by orders of magnitude do not compare equal, in `io/src/test/java/au/csiro/pathling/io/annotation/QuantityCanonicalAnnotationTest.java`.
- [ ] T055 [P] [US1] Test that each annotation can be disabled individually and that disabling one does not affect the others, in `io/src/test/java/au/csiro/pathling/io/annotation/AnnotationToggleTest.java`.
- [ ] T056 [P] [US1] Test that `contained` resources are detected and governed by the strictness switch, never silently dropped, in `io/src/test/java/au/csiro/pathling/io/transform/StrictnessTest.java`.
- [ ] T057 [P] [US1] Test that content the definition set does not describe is ignored or raises, per the switch, in the same file. Detection must compare observed keys against the definitions: the JSON reader silently skips unknown fields in every mode, so it cannot be asked to enforce this.
- [ ] T057a [P] [US1] Test that input whose cardinality contradicts the definitions — a repeating element supplied as a single object rather than a one-element array — is governed by the strictness switch and never silently coerced, in the same file. Conformant FHIR JSON always uses an array for a repeating element, so inference gets cardinality right for conformant input; this is the case where it does not, and it surfaces in the transform rather than at the read.
- [ ] T058 [P] [US1] Test that a bundle is exploded to per-type tables and is never stored as a resource type, in `io/src/test/java/au/csiro/pathling/io/transform/BundleTransformTest.java`.

### Implementation

- [ ] T059 [US1] Implement the JSON read and the transform into the derived schema in `io/src/main/java/au/csiro/pathling/io/transform/ResourceTransformer.java`. Read with an inferred schema, then impose types, cardinality and conventions from the definitions (R-008).
- [ ] T060 [P] [US1] Implement the decimal transform in `io/src/main/java/au/csiro/pathling/io/transform/DecimalTransform.java`.
- [ ] T061 [P] [US1] Implement the primitive metadata transform in `io/src/main/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransform.java`.
- [ ] T062 [P] [US1] Implement the extension transform in `io/src/main/java/au/csiro/pathling/io/transform/ExtensionTransform.java`.
- [ ] T063 [US1] Implement the annotation processor registry in `io/src/main/java/au/csiro/pathling/io/annotation/AnnotationProcessors.java`, with each processor individually enableable.
- [ ] T064 [P] [US1] Implement the numeric annotation in `io/src/main/java/au/csiro/pathling/io/annotation/NumericAnnotation.java`.
- [ ] T065 [P] [US1] Implement the date range annotation in `io/src/main/java/au/csiro/pathling/io/annotation/DateRangeAnnotation.java`.
- [ ] T066 [P] [US1] Implement the quantity canonical annotation in `io/src/main/java/au/csiro/pathling/io/annotation/QuantityCanonicalAnnotation.java`, carrying the wider arbitrary-scale representation rather than the specification's fixed-point one.
- [ ] T067 [US1] Implement strictness checking against the definitions, including `contained` detection, in `io/src/main/java/au/csiro/pathling/io/transform/StrictnessCheck.java`.
- [ ] T068 [US1] Implement bundle explosion and reference resolution within a bundle in `io/src/main/java/au/csiro/pathling/io/transform/BundleTransformer.java`.
- [ ] T069 [US1] Implement XML ingest by parsing with FHIR tooling in a UDF and handing JSON to the transform, in `io/src/main/java/au/csiro/pathling/io/transform/XmlIngest.java`.
- [ ] T070 [US1] Wire the transform into `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signatures (FR-043).

**Checkpoint**: Data is written in the new layout. The engine cannot read it yet.

---

## Phase 5: User Story 2 - Lossless round trip (Priority: P1)

**Goal**: Conformant FHIR JSON round-trips to a semantically equal resource.

**Independent Test**: Round-trip a real corpus, asserting semantic equality
resource by resource, with no FHIRPath evaluation.

### Tests ⚠️ write first, confirm failing

- [ ] T071 [US2] Build the round-trip harness in `io/src/test/java/au/csiro/pathling/io/RoundTripHarness.java`, comparing semantically: object key order ignored, array order significant, numbers compared lexically.
- [ ] T072 [P] [US2] Test decimals through the harness: a trailing zero, exponent notation, a leading sign, a very small magnitude and forty significant digits, in `io/src/test/java/au/csiro/pathling/io/DecimalRoundTripTest.java`.
- [ ] T073 [P] [US2] Test that an absent element is absent from the output rather than present and null, in `io/src/test/java/au/csiro/pathling/io/EgressOmissionTest.java`.
- [ ] T074 [P] [US2] Test that a structure whose every field is null in a row is omitted rather than serialised as an empty object, in the same file. This is the default serialisation behaviour, so it will fail before the fix.
- [ ] T075 [P] [US2] Test that an array whose every element is null is omitted rather than serialised as an array of nulls, in the same file.
- [ ] T076 [US2] Run the harness over the FHIR R4 specification examples in `io/src/test/java/au/csiro/pathling/io/SpecExampleRoundTripTest.java`.
- [ ] T077 [US2] Run the harness over a Synthea corpus in `io/src/test/java/au/csiro/pathling/io/SyntheaRoundTripTest.java`.
- [ ] T078 [P] [US2] Test and thereby pin the documented limitation: resources supplied as a dataset of strings do not preserve decimal lexical form, in `io/src/test/java/au/csiro/pathling/io/StringDatasetLimitationTest.java`. Asserting it stops the limitation silently widening.
- [ ] T078a [P] [US2] Test that on a pruned schema the round-trip guarantee holds unconditionally, and that on a dense schema content the configured nesting, extension or open-type bounds would drop is detectable rather than silently lost (FR-017), in `io/src/test/java/au/csiro/pathling/io/DenseBoundsDetectionTest.java`.

### Implementation

- [ ] T078b [US2] Implement detection of content the dense bounds would drop, reported through the strictness switch, in `io/src/main/java/au/csiro/pathling/io/transform/BoundsCheck.java`.
- [ ] T079 [US2] Implement egress from the layout to JSON in `io/src/main/java/au/csiro/pathling/io/egress/ResourceSerialiser.java`.
- [ ] T080 [US2] Implement omission of all-null structures and null-only arrays in `io/src/main/java/au/csiro/pathling/io/egress/EmptyPruning.java`.
- [ ] T081 [US2] Wire egress into the decode entry point in `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signature.
- [ ] T082 [P] [US2] Wire egress into `library-api/src/main/java/au/csiro/pathling/library/io/sink/NdjsonSink.java`.

**Checkpoint**: Driver 2 is demonstrated over a real corpus.

---

## Phase 6: User Story 3 - The engine reads the new layout (Priority: P1)

**Goal**: Existing expressions, views and searches return the same answers over
the new layout.

**Independent Test**: The FHIRPath suite, both conformance baselines and the
SQL-on-FHIR compliance suite pass over data in the new layout.

*T083–T088 (Coding by name) are correct under both layouts and can land at any
time, including before Phase 4.*

### Tests ⚠️ write first, confirm failing

- [ ] T083 [P] [US3] Test that a full canonical Coding structure decodes as it does today, that a `{code, system}` structure decodes with the remaining properties null, and that reordered fields follow names rather than positions, in `terminology/src/test/java/au/csiro/pathling/fhirpath/encoding/CodingSchemaTest.java`.
- [ ] T084 [P] [US3] Test that a structure carrying no recognisable Coding field is rejected with an error naming the expected and the actual fields, in the same file.
- [ ] T085 [P] [US3] Test each terminology operation against a narrowed Coding column, in `terminology/src/test/java/au/csiro/pathling/sql/udf/NarrowCodingTest.java`.
- [ ] T086 [P] [US3] Test that decimal comparison, arithmetic and ordering over lexically stored decimals match the current results, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/DecimalCollectionTest.java`.
- [ ] T087 [P] [US3] Test that cross-unit quantity comparison works with the canonical annotation present and computes it when absent, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/QuantityCollectionTest.java`.
- [ ] T088 [P] [US3] Test that `resolve()` works without a stored versioned-key column, reusing the fixtures from T020–T023, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/ResolveFunctionDslTest.java`.
- [ ] T089 [US3] Test that the full suite passes over files written with every annotation disabled (FR-022, SC-004), in `fhirpath/src/test/java/au/csiro/pathling/test/AnnotationFreeSuiteTest.java`.
- [ ] T090 [US3] Test that whether an annotation is used is decided from the schema rather than per row (FR-023), by asserting the generated plan differs between an annotated and an unannotated input, in the same file.

### Implementation

- [ ] T091 [US3] Resolve Coding fields by name against the schema of the row being decoded, resolved once per schema rather than per row, in `terminology/src/main/java/au/csiro/pathling/fhirpath/encoding/CodingSchema.java`. Absent fields decode as null.
- [ ] T092 [US3] Route the terminology helpers through the resolver in `terminology/src/main/java/au/csiro/pathling/sql/udf/TerminologyUdfHelpers.java`.
- [ ] T093 [P] [US3] Remove index-based assumptions from `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/CodingCollection.java`, `fhirpath/src/main/java/au/csiro/pathling/fhirpath/FhirPathType.java` and `library-api/src/main/java/au/csiro/pathling/library/TerminologyHelpers.java`.
- [ ] T094 [US3] Implement sibling resolution — resolve a named sibling of a primitive within its parent structure — in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, retaining the parent handle and element name on traversal to a primitive. This one mechanism serves both annotations and primitive metadata (R-014).
- [ ] T095 [US3] Move decimal decoding to the lexical representation with the numeric annotation as a fast path, keeping query-time precision unchanged, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/DecimalCollection.java`.
- [ ] T096 [US3] Move quantity handling to the FHIR structure with the wide canonical annotation, computing canonicalisation when the annotation is absent, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/encoding/QuantityEncoding.java` and `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/QuantityCollection.java`.
- [ ] T097 [US3] Replace field-identifier-based extension access with inline extension traversal in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/Collection.java` and `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/ResourceCollection.java`.
- [ ] T098 [US3] Compute reference keys from the conformant identifier elements rather than a stored versioned-key column, in the reference resolution and join machinery under `fhirpath/src/main/java/au/csiro/pathling/fhirpath/`.
- [ ] T099 [P] [US3] Replace the canonicalised-quantity field-name constants in `fhirpath/src/main/java/au/csiro/pathling/search/filter/FhirFieldNames.java` and update the search matchers that use them.
- [ ] T100 [US3] Run the full FHIRPath suite, both YAML conformance baselines and the SQL-on-FHIR compliance suite over the new layout (SC-002); remove exclusion entries that have become obsolete rather than leaving them to self-report (SC-003). Confirm the engine now reads only the new layout and no engine code path reads the previous one (FR-053).

**Checkpoint**: The engine reads the new layout. The old encoder is no longer read by the engine.

---

## Phase 7: User Story 4 - Queries over a fitted schema (Priority: P1)

**Goal**: Expressions naming elements absent from the schema evaluate to empty
rather than failing, and view output types stay predictable.

**Independent Test**: Run the suite in pruned mode; assert an expression over an
absent element yields empty.

### Tests ⚠️ write first, confirm failing

- [ ] T101 [P] [US4] Test that traversal to an element the definitions describe but the schema lacks yields an empty collection, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`.
- [ ] T102 [P] [US4] Test that traversal to an element the definitions do not describe still raises an error, in the same file.
- [ ] T103 [P] [US4] Test that selecting a choice variant absent from the schema yields empty, in the same file.
- [ ] T104 [P] [US4] Test that combining an absent element with a populated one succeeds — union, combination, conditional selection and comparison (FR-027), in the same file.
- [ ] T104a [P] [US4] Test that two collections of the same FHIR type reached by different paths, whose fitted schemas differ, combine successfully — holding every element of both and remaining traversable (FR-056) — and that the result's fields are in definition order (FR-057), in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/operator/ShapeReconciliationTest.java`. Cover `combine`, `|`, equality of complex values, conditional selection and membership.
- [ ] T105 [P] [US4] Test that sibling column combination tolerates a bottom-typed complex element, in `fhirpath/src/test/java/au/csiro/pathling/projection/SiblingCombinationTest.java`. No *primitive* is untyped under FR-055, but an absent complex element is the bottom type, and the recursive selection path computes an expected element type that now meets it where it previously met a statically empty collection.
- [ ] T106 [P] [US4] Test that a view column declaring a FHIR type produces an output column of that type over an absent element (FR-028), in `fhirpath/src/test/java/au/csiro/pathling/projection/ProjectedColumnTypeTest.java`.
- [ ] T107 [P] [US4] Test that a view column declaring no type over an absent *primitive* element succeeds, carrying the type the definitions give the element (FR-030), and that deriving an output type still fails with a message naming the column, its path and the remedy for a column that genuinely carries no type information (FR-029), in the same file. An element absent from the schema must no longer reach that failure.
- [ ] T108 [P] [US4] Test that one column expression, built with no reference to any dataset, applied to a fitted dataset and to a dense dataset holding the same resources, yields equal results for every expression in the engine's expression-level test set (FR-054, SC-010), in `library-api/src/test/java/au/csiro/pathling/library/FhirPathToColumnTest.java`. This is the assertion that the expression-to-column API's unchanged signature is honest.
- [ ] T108a [P] [US4] Test that a column over an absent primitive element reaching a caller through the expression-to-column API carries the element's type and can be written to Parquet (FR-030), in the same file.
- [ ] T109 [US4] Run the shape-sensitive tests with an explicit expected schema assertion, since a pruned schema is derived from the whole fixture set and adding a fixture can silently flip an assertion from a null branch to a missing-field branch.

### Implementation

- [ ] T110 [US4] Emit the tolerant traversal expression from T038e at **every** traversal step, replacing the direct field reference, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, with the fallback type taken from the element definition per FR-055. It must be every step, not only where absence is suspected: traversal into a fallback fails for every possible fallback type, so the tolerance has to intercept before a direct field reference is ever emitted over one.
- [ ] T111 [US4] Apply the declared FHIR type as a cast on output, not only the explicit SQL-type tag, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T112 [US4] Improve the message raised when no type information is available to name the column, its path and the remedy, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T113 [US4] Ensure sibling combination tolerates a bottom-typed complex element in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectionResult.java` and `fhirpath/src/main/java/au/csiro/pathling/projection/RepeatSelection.java`.
- [ ] T113a [US4] Apply the reconciliation expression from T038l where operands are prepared for combination, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/CombiningLogic.java`, generalising what `prepareArray` already does for `DecimalCollection` (FR-056). The operators themselves do not change, and the FHIR-type promotion in `FhirPathBinaryOperator.reconcileTypes` stays a separate, definition-driven step ahead of it.
- [ ] T113b [US4] Apply reconciliation at the remaining sites that need two operands to share a SQL type — conditional selection and membership — and at `ColumnRepresentation.traverseChoice`, which coalesces across several variant fields at once and therefore passes the whole ordered operand list rather than folding (FR-056).

**Checkpoint**: The pruned schema is usable from every query surface.

---

## Phase 8: User Story 5 - Divergent files read as one dataset (Priority: P2)

**Goal**: Batches written at different times query as one dataset without loss.

**Independent Test**: Write two batches with divergent schemas; query across both
and assert every resource appears with correct cardinality.

### Tests ⚠️ write first, confirm failing

- [ ] T114 [P] [US5] Test that reading files with differing schemas returns the union of their elements with no resource lost, in `library-api/src/test/java/au/csiro/pathling/library/io/source/SchemaMergingTest.java`.
- [ ] T115 [P] [US5] Test that appending a batch whose schema differs from the table's succeeds and widens the table, in `library-api/src/test/java/au/csiro/pathling/library/io/sink/DeltaSchemaMergingTest.java`.
- [ ] T116 [US5] Test that a view unnesting a repeating element and projecting a leaf only one batch populated returns resources from both, extending T026–T027 to the source and sink level, in `library-api/src/test/java/au/csiro/pathling/library/io/source/DivergentFileViewTest.java`.

### Implementation

- [ ] T117 [P] [US5] Enable schema merging on append in `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`.
- [ ] T118 [P] [US5] Enable schema merging on read, with an opt-out, in `library-api/src/main/java/au/csiro/pathling/library/io/source/ParquetSource.java`.
- [ ] T118a [US5] Where a merged schema is produced rather than obtained from Spark, produce it with the shared merge from T038g so field order stays canonical (FR-057, FR-058). A merge that appends newly discovered fields in discovery order rather than definition order yields structures that cannot be combined and, worse, compare positionally.
- [ ] T119 [US5] Measure merge cost over a realistic file count on object storage and record it in `evidence/merge-cost.md`. The result can force a fallback for raw files (R-015).

**Checkpoint**: Incremental loading works with schemas fitted to data.

---

## Phase 9: User Story 6 - Primitive ids and extensions in FHIRPath (Priority: P2)

**Goal**: Expressions can navigate to a primitive element's id and extensions.

**Independent Test**: Evaluate such expressions over fixtures carrying them.

### Tests ⚠️ write first, confirm failing

- [ ] T120 [P] [US6] Test navigation to a primitive element's extensions, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/PrimitiveMetadataNavigationTest.java`.
- [ ] T121 [P] [US6] Test navigation to a primitive element's id, in the same file.
- [ ] T122 [P] [US6] Test that both yield empty where the source carried neither, in the same file.
- [ ] T123 [P] [US6] Add fixtures carrying primitive ids and extensions under `fhirpath/src/test/resources/viewTests/`.

### Implementation

- [ ] T124 [US6] Implement navigation to the metadata group through the sibling resolution added in T094, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/Collection.java`.

**Checkpoint**: Driver 3 is delivered end to end — stored and readable.

---

## Phase 10: User Story 8 - The module boundary (Priority: P3)

**Goal**: The new encoding path carries no dependency on internal Spark Catalyst
API, and the existing implementation is untouched.

**Independent Test**: The build fails when the boundary is violated.

*Mostly delivered by T004–T009; this phase verifies it.*

- [ ] T125 [P] [US8] Verify that adding a Catalyst dependency to `fhir-schema/pom.xml` fails the build, and revert.
- [ ] T126 [P] [US8] Verify that adding a Catalyst import to the new encoding code fails the build, and revert.
- [ ] T127 [US8] Confirm by inspection that the new encoding path contains no expression encoder, no hand-authored serializer or deserializer expression tree and no FHIR object in a per-row plan (FR-050).
- [ ] T128 [US8] Confirm `encoders` is unmodified — `git diff main -- encoders/` is empty — and that `mvn -pl encoders` still resolves (FR-051). This also establishes FR-052: the query-time expression toolkit lives in `encoders` and has therefore been neither changed nor relocated.

**Checkpoint**: Driver 5 is delivered for the encoding path and enforced by the build.

---

## Phase 11: Polish & Cross-Cutting Concerns

- [ ] T129 [P] Rewrite `site/docs/libraries/io/schema.md` from [contracts/storage-layout.md](contracts/storage-layout.md), including the fitted-schema statement, the unnesting caution for direct SQL consumers and the compatibility statement for existing files.
- [ ] T130 [P] Document the new options and the behaviour changes from [contracts/library-api.md](contracts/library-api.md) in `site/docs/libraries/`, including the decimal limitation on the string-dataset path, that a column expression remains valid over any conformant schema with absent primitives carrying their definition-derived type, and that a column over an absent *complex* element is bottom-typed and so cannot be written to Parquet and is omitted in JSON.
- [ ] T131 [P] Update the module list and dependency diagram in `CONTRIBUTING.md`, and the build-order line in `.claude/CLAUDE.md`.
- [ ] T132 [P] File the upstream Spark issue as a follow-up to SPARK-48148, with the reproduction from `evidence/spark-type-findings.md`: exact string parsing covers only byte-array and positionally-readable content references, so the string-dataset and `from_json` paths still route numbers through a double.
- [ ] T133 [P] Raise the follow-up issue recording the two alternative bindings from R-017 as future options — evaluation-time schema binding, and an unbound column representation bound when the dataset is known. Note what each would add beyond what is built here: bind-time validation and a required-schema manifest, and therefore Pathling-level messages naming the missing element instead of a Spark analysis failure. Note also that they remain compatible, because the representation of absence and the reconciliation mechanism are shared. Typed absent elements are **no longer** a follow-up: FR-055 delivers them for primitives.
- [ ] T134 [P] Raise the follow-up issue for the decimal quoting pre-pass (R-009), recording that a blanket approach fails because a quoted number against an integer target is rejected.
- [ ] T135 Benchmark the two ingest mechanisms against each other — the chosen transform approach and direct parsing into a variant — and record the comparison in `evidence/ingest-comparison.md`. This decides whether the lexical-decimal limitation is worth removing by changing mechanism (R-009).
- [ ] T136 Re-run the split benchmark and compare against the T003 baseline. Record encode, decode, planning and execution separately. No outcome is required (SC-007).
- [ ] T137 Correct or retire `openspec/parquet-on-fhir-design.md`, which this specification supersedes: the stored decimal precision is wrong, the nested-pruning risk does not apply to the engine and its proposed mitigation was insufficient, and the first argument against schema inference does not hold for FHIR JSON.
- [ ] T138 Remove or archive the five superseded change directories under `openspec/changes/` belonging to this programme.
- [ ] T139 Run every scenario in [quickstart.md](quickstart.md) as final validation.

---

## Traceability

Every functional requirement in [spec.md](spec.md) and the task or tasks that
deliver it. Checked mechanically as part of the consistency pass; re-check it
when a task is added or removed.

| Requirement | Tasks |
| --- | --- |
| FR-001 Layout conformance | T050–T070 |
| FR-002 Decimals lexical plus annotation | T050, T060, T064 |
| FR-003 Primitive metadata groups, inline extensions, no identifier or map | T051, T052, T061, T062 |
| FR-004 Date range annotations | T053, T065 |
| FR-005 Magnitude-preserving quantity annotation | T054, T066 |
| FR-006 `contained` excluded and detected | T056, T067 |
| FR-007 `Bundle` never stored | T058, T068 |
| FR-008 Schema derived from definitions | T028, T031 |
| FR-009 Pruned and dense modes | T029, T032, T033 |
| FR-010 Dense is the same derivation unpruned | T030 |
| FR-011 No field-less structure | T029 |
| FR-012 Types and cardinality never from data | T028 |
| FR-013 Child enumeration | T013, T016 |
| FR-014 Cardinality exposed | T014, T017 |
| FR-015 Version-independent type representation | T015, T018 |
| FR-016 Round-trip equality | T071, T076, T077 |
| FR-017 Unconditional on pruned, bounded and detectable on dense | T078a, T078b |
| FR-018 Strictness switch | T057, T067 |
| FR-019 Absent elements omitted on export | T073, T074, T075, T080 |
| FR-020 Limitations documented per path | T078, T130 |
| FR-021 Annotations default on, individually disableable | T055, T063 |
| FR-022 Engine computes when annotations absent | T089 |
| FR-023 Fast path chosen from the schema | T090 |
| FR-024 Absent element yields empty | T101, T110 |
| FR-025 Undefined element raises | T102 |
| FR-026 Absent choice variant yields empty | T103 |
| FR-027 Absent combines with populated | T104, T105, T113 |
| FR-028 Declared FHIR type applied to output | T106, T111 |
| FR-029 Untyped derivation fails with an actionable message, and an absent element is not such a column | T107, T112 |
| FR-030 An absent primitive carries its definition-derived type | T107, T108a, T130 |
| FR-031 Coding decoded by name | T083, T084, T091 |
| FR-032 Terminology on narrowed Coding | T085, T092, T093 |
| FR-033 Reference keys without a stored column | T088, T098 |
| FR-034 Primitive id and extension navigation | T120–T124 |
| FR-035 Query-time decimal precision unchanged | T086, T095, T130 |
| FR-036 Unnesting never reduced to a single leaf | T026, T027, T116 |
| FR-054 Traversal tolerant of an absent field, decided after schema resolution | T038a, T038b, T038d, T038e, T110, T108 |
| FR-055 Absent elements typed by the definitions or the bottom type | T038c, T038e, T105, T110 |
| FR-056 Reconciliation by by-name projection, never by cast | T038j, T038k, T038l, T104a, T113a, T113b |
| FR-057 Canonical definition field order wherever a structure type is produced | T030a, T038h, T038i, T104a, T118a |
| FR-058 One recursive field-wise merge serving reconciliation and file merging | T038f, T038g, T118a |
| FR-037 Unsupported layouts rejected with an actionable message | T042, T045, T046, T047 |
| FR-038 Detection structural, no extra file access | T044 |
| FR-039 Sparse and unclassifiable schemas accepted | T041 |
| FR-040 Detection disableable per source | T043, T048, T049 |
| FR-041 Divergent files read as one dataset | T114, T118 |
| FR-042 Divergent append widens the table | T115, T117 |
| FR-043 Encoding and decoding APIs preserved | T070, T081 |
| FR-044 Bounds options apply to dense only | T033 |
| FR-045 XML and Bundle ingest preserved | T058, T068, T069 |
| FR-046 Earlier layouts not read as the new one | T042, T045 |
| FR-047 Published layout contract replaced | T129 |
| FR-048 Dependency ban on the schema module | T005, T125 |
| FR-049 Import check on the new encoding | T007, T126 |
| FR-050 No encoder or hand-authored expression trees | T127 |
| FR-051 Existing implementation untouched | T128 |
| FR-052 Query toolkit preserved and not relocated | T128 |
| FR-053 Engine reads only the new layout | T100 |

And the success criteria:

| Criterion | Tasks | Quickstart |
| --- | --- | --- |
| SC-001 Round trip over a real corpus, including the five decimal forms | T072, T076, T077 | QS-001 |
| SC-002 Suites pass over the new layout in pruned mode, dense subset green | T100, T037, T038 | QS-003 |
| SC-003 Exclusion baselines gain no entries, obsolete ones removed | T100 | QS-003 |
| SC-004 Suite passes with every annotation disabled | T089 | QS-004 |
| SC-005 Divergent files return every resource with correct cardinality | T114, T115, T116 | QS-006 |
| SC-006 Earlier layouts rejected at read time with an actionable message | T042, T045–T047 | QS-008 |
| SC-007 Encode, decode, planning and execution measured separately | T002, T003, T136 | QS-010 |
| SC-008 No incompatible public API change in Java, Python or R | T049, T070, T081, T082 | QS-002, QS-008 |
| SC-009 Build fails on internal Catalyst API in the schema module | T005, T125, T126 | QS-009 |
| SC-010 One column expression gives equal results on a fitted and a dense dataset | T038a, T038b, T108 | QS-005 |

## Dependencies & Execution Order

- **Setup (Phase 1)**: no dependencies. T001 must run before anything else — the baseline is unrecoverable once modules move.
- **Foundational (Phase 2)**: depends on Setup; blocks every user story.
  - T010–T012 (motion) before T013–T019 (widening).
  - T020–T027 (coverage) depend only on Setup and can run in parallel with the definition work. They must complete before Phase 6, which changes the conventions they protect.
  - T028–T033 (derivation) depend on T016–T018.
  - T034–T038 (test infrastructure) depend on T031.
- **US7 (Phase 3)**: depends only on Setup. Sequenced first despite its priority, because it is correct against the current codebase and should exist before there is anything for it to catch.
- **US1 (Phase 4)**: depends on Foundational.
- **US2 (Phase 5)**: depends on US1 — there is nothing to round-trip until data is written.
- **US3 (Phase 6)**: depends on US1 and on T020–T027. T083–T093 (Coding by name) are correct under both layouts and may land at any point.
- **US4 (Phase 7)**: depends on US3.
- **US5 (Phase 8)**: depends on US1; independent of US3 and US4 except for T116, which needs US4.
- **US6 (Phase 9)**: depends on T094 in US3.
- **US8 (Phase 10)**: verification only; depends on Setup and US1.
- **T038a gates the approach.** It is the only untested risk in the chosen
  design: if the tolerant traversal expression cannot survive the analyzer over
  an unresolved child, the fallback is option C in R-017 — the unbound column
  representation — and T038e, T038l, T110, T113a and T113b are rewritten against
  it. The representation of absence (FR-055) and the reconciliation mechanism
  (FR-056 to FR-058) are unaffected either way, which is why this gate is cheap
  to fail. Run it before anything in Phases 4 to 9 is built on it.
- **Polish (Phase 11)**: depends on the stories it documents. T132–T134 may be raised at any time.

Within each story: tests written and failing, then implementation. Tasks touching
the same file run sequentially; `[P]` tasks on different files may run together.

## Implementation Strategy

Land US7 first — it is correct today, independent, and converts the worst failure
mode in the programme into a message.

Then the MVP is Setup, Foundational, US1, US2: data is written in the new layout
and demonstrated lossless over a real corpus, with the engine still on the old
path. That is a complete, defensible increment even if nothing else lands.

US3 is the flag day. The engine moves to the new layout and the test estate moves
with it, in one step, through the category-C fixture path. US4 follows
immediately, because a fitted schema is not usable until absent elements behave.

US5, US6 and US8 are independent afterwards.

The server pins to the last library release before US3 — the first change to the
conventions — and must reach the post-change line before the pinned line stops
receiving fixes.
