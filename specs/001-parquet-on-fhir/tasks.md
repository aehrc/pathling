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

**Organisation**: Tasks are grouped into milestones, and within a milestone by
user story, so each story can be implemented and tested independently.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: can run in parallel — different files, or independent test methods
  within one test file, and no dependency on an incomplete task.
- **[Story]**: the user story (US1–US8); omitted for Setup, the shared
  foundations, the public API switch and Polish.

---

## Milestones

Six. **M1, M3 and M6 change nothing a user can observe, and each is independently
shippable. M2 changes two things, both in Phase 10 and both named in its row.
M4 is the only breaking release.** M5 lands as a series
of independent increments: each annotation kind ships on its own, and primitive
ids and extensions follow them.

**Task IDs are never renumbered and never reused.** They are referenced
throughout this document and by both traceability tables, so an addition takes a
letter suffix and sits beside the task it belongs with. Identifiers therefore do
not run in numeric order, and the phase map below rather than the numbering is
what says which tasks are where.

| Milestone | Phases | Tasks | Delivers | User-visible change |
| --- | --- | --- | --- | --- |
| **M1 The layout** | 1–6 | 61 | FHIR JSON to the new layout and back through `io`, losslessly except primitive ids and extensions. No annotations. Schema derivation, the canonical structure and the one shared merge. Both risk gates resolved | None |
| **M2 The engine, layout-tolerant** | 7–10 | 69 | The engine reads both layouts by dispatch, computes every value without annotations, tolerates a fitted schema, and reads divergent files as one dataset. Green on the previous layout throughout | Two, both in Phase 10: the Delta upsert path widens the target where it used to refuse (T117a, decision 41), and reading raw files merges their schemas with a supplied-schema opt-out (T118). Move those three tasks to M4 if the milestone must be invisible |
| **M3 Ingest formats** | 11 | 5 | Bundles and XML on the new path, each round-tripped through the M1 harness | None |
| **M4 The flip** | 12–14 | 28 | The public API writes the new layout, earlier layouts are detected, the layout is documented and the benchmark is recorded | The flag day |
| **M5 The gaps** | 15–16 | 21 | Annotations emitted and read, one kind at a time; primitive ids and extensions written and navigable | Performance, then new capability |
| **M6 Completion** | 17 | 13 | The previous-layout reader removed, FR-053 tightened, the superseded design retired, follow-ups raised | None, **contingent on T049a**: if the source boundary routes earlier-layout data rather than refusing it, the reader T100e removes is a product feature and its removal is breaking |

### Phase map

| Phase | Tasks | Count |
| --- | --- | --- |
| 1 Setup | T001–T009, T009a | 10 |
| 2 The definition abstraction | T010–T019 | 10 |
| 3 Schema derivation | T028, T029, T030, T030a, T031, T032, T033a, T033, T033b, T038f–T038i, T119 | 14 |
| 4 US1 The storage layout | T050, T052, T056, T057, T057a, T057b, T059, T060, T062, T067, T067a | 11 |
| 5 US2 Lossless round trip | T071–T078, T078a, T078b, T079, T080 | 12 |
| 6 US8 The module boundary | T125–T128 | 4 |
| 7 Engine foundations | T020–T027, T027a, T049a, T034–T038, T037a, T038a–T038e, T038j–T038m, T110a, T101–T107, T110–T113 | 37 |
| 8 US3 The engine reads both layouts | T083–T089, T089a, T091–T094, T094a, T094b, T095–T099 | 19 |
| 9 US4 Shape reconciliation (US4's absent-element half is in Phase 7) | T104a, T109, T113a, T113b | 4 |
| 10 US5 Divergent files read as one dataset | T114–T117, T117a, T118, T118a–T118c | 9 |
| 11 Remaining ingest formats | T058, T058a, T068, T069, T069a | 5 |
| 12 The public API switch | T070, T081, T082, T100, T100b, T100c, T100d, T100g, T108, T108a, T128a | 11 |
| 13 US7 Detection of earlier layouts | T039, T039a, T040–T042, T042a, T043–T046, T046a, T047–T049 | 14 |
| 14 Documentation and measurement | T129, T130, T136 | 3 |
| 15 Annotations | T050a, T053–T055, T063–T066, T086a, T087a, T090, T095a, T096a | 13 |
| 16 US6 Primitive ids and extensions | T051, T061, T078c, T120–T124 | 8 |
| 17 Completion | T100f, T100e, T131–T134, T134a–T134c, T135, T137–T139 | 13 |

**M1 and M3 change nothing a user can observe, and M2 changes only what its
milestone row names.** `PathlingContext.encode`, `PathlingContext.decode` and
`NdjsonSink` keep writing and reading the previous layout throughout; the new
path is reachable only through `io`'s own entry points and the test estate. This
is why T070, T081 and T082 — the public API rewiring — sit in M4 rather than in
US1 and US2 where the rest of their stories live. What M2 does change is the two
sink and source behaviours in Phase 10, which are reachable through the public
API and are listed against it.

**M2 keeps the engine green on the previous layout.** Its conversion is by
addition rather than replacement: each traversal expression dispatches on the
resolved schema, so the engine reads both layouts for the length of the
transition. The test framework gains two dimensions rather than one — the schema
mode (T037) and the layout (T037a) — because how much of a layout a schema
carries and which conventions its fields follow are separate questions, and only
the first was ever addressed. Tolerant traversal (T110) lands ahead of both, so
neither dimension can be switched onto something the engine cannot read.
**There is therefore no window without a green build**, which reverses decision
40. Decision 52 records the three axes this rests on.

**M4 is the flag day.** The public API switches, detection of earlier layouts
lands beside it, and the layout is documented. Everything it depends on — the
engine reading both layouts, a fitted schema behaving, divergent files reading as
one dataset — is already in place.

**M5 closes two gaps deliberately left open.** The flip lands annotation-free
and without primitive ids or extensions. FR-022 makes the first safe, because an
annotation is only ever a fast path and never required for correctness; FR-017
carries a carve-out for the second until T078c closes it. T136 at the flip is
what orders the work, per FR-033.

**The definition abstraction and schema derivation belong to M1**, not to the
engine, because the ingest transform writes *into* the derived schema and
FR-008/FR-012 forbid taking types or cardinality from the data. The structure
merge and the canonical structure (T038f–T038i) are in M1 for the same reason. The
merge and its interface are pure structure mechanics over `spark-sql-api` types
and live in `utilities`, so that `encoders` reaches the interface without
depending on `fhir-schema`. The definition-backed implementation sits beside
`SchemaBuilder`, so derivation and merging share one notion of canonical order by
construction rather than two that can drift.

---

# Milestone 1 — The layout

Data conforming to Parquet on FHIR is written and read back losslessly.
Primitive ids and extensions and every annotation are deliberately left to M5.
Nothing a user can see changes.

## Phase 1: Setup

**Purpose**: Capture the baseline and create the module scaffolding. No
behaviour changes.

- [x] T001 Run the JMH suite on unmodified `main` in `benchmark/src/main/java/au/csiro/pathling/benchmark/PathlingBenchmark.java` and record the results in `evidence/baseline.md`, noting the machine, fork count and Spark version. **Must precede every other task**: the baseline and the comparison must come from the same machine under the same conditions, and capturing it first is the only way to guarantee that. The measurement itself stays recoverable from `main` through a worktree.
- [x] T002 Split the benchmark so encode, decode and query execution are timed separately, and planning time separately from execution time, in `benchmark/src/main/java/au/csiro/pathling/benchmark/PathlingBenchmark.java`. The current benchmarks report one number for an NDJSON-to-view pipeline, which cannot answer driver 1 either way.
- [x] T003 Re-run T001 against the split benchmark and record the per-phase baseline in `evidence/baseline.md`.
- [x] T004 Create the `fhir-schema` module with `fhir-schema/pom.xml`, depending on `utilities` and `spark-sql-api` only.
- [x] T005 Add an enforcer rule to `fhir-schema/pom.xml` banning `org.apache.spark:spark-catalyst_*` and `org.apache.spark:spark-sql_*`, and prove the rule works by violating it deliberately on the empty module. T125 re-checks it against real code.
- [x] T006 [P] Create the `io` module with `io/pom.xml`, depending on `fhir-schema` and `spark-sql`.
- [x] T007 [P] Add a build rule to `io/pom.xml` failing on any `org.apache.spark.sql.catalyst` import, and prove the rule works by violating it deliberately on the empty module. T126 re-checks it against real code.
- [x] T008 Add both modules to `<modules>` in `pom.xml` in build order, and to the dependency list in `library-runtime/pom.xml` so the Python and R libraries ship them.
- [x] T009 Add `fhir-schema` and `io` as dependencies of `fhirpath` in `fhirpath/pom.xml`. `encoders/pom.xml` is not modified.
- [x] T009a **Spike, and the gate on the whole approach.** Build a throwaway `RuntimeReplaceable` over an *unresolved* attribute and confirm it survives the analyzer — that nothing probes `dataType` before the child resolves. Discard the code; T038a is the durable test. Run it here rather than in Phase 7 because it is the programme's largest unknown and costs a day, so the answer is worth having while M1 still has planning time. **On failure**: the design falls back to the unbound column representation in R-017 option C, and T038e, T038l, T110, T113a and T113b are rewritten against it before M2 opens. FR-055 to FR-058 are unaffected either way.

**Checkpoint**: Modules exist and are empty; the boundary is enforced; the baseline is captured; the analyzer risk is resolved.

---

## Phase 2: The definition abstraction

**Purpose**: Move the definitions below the engine and widen them enough to drive
schema derivation. Pure motion first, then widening.

- [x] T010 Move `fhirpath/src/main/java/au/csiro/pathling/fhirpath/definition/**` (including `defaults/` and `fhir/`) to `fhir-schema/src/main/java/au/csiro/pathling/definition/`. Pure motion — no test task; behaviour is unchanged by construction.
- [x] T011 Update the referencing files in `fhirpath` main and test to the new package.
- [x] T012 Verify the motion by reviewing the test diff: it must contain only package declarations and import statements. This is the test for T010–T011.
- [x] T013 Test child enumeration in `fhir-schema/src/test/java/au/csiro/pathling/definition/NodeDefinitionTest.java` — children of a resource, a backbone element and a complex type, asserting order and completeness against the FHIR definitions, and a choice element appearing once as a choice rather than pre-expanded.
- [x] T014 Test cardinality in `fhir-schema/src/test/java/au/csiro/pathling/definition/ElementDefinitionTest.java` — singular, repeating, and choice elements.
- [x] T015 Test that a type code outside the R4 enumeration is representable, and that both implementations agree on enumeration and cardinality for the same resource, in `fhir-schema/src/test/java/au/csiro/pathling/definition/DefinitionContextAgreementTest.java`.
- [x] T016 Add child enumeration to `fhir-schema/src/main/java/au/csiro/pathling/definition/NodeDefinition.java`, implemented from the children the HAPI-backed implementation already obtains rather than asking a second time.
- [x] T017 Add cardinality to `fhir-schema/src/main/java/au/csiro/pathling/definition/ElementDefinition.java`.
- [x] T018 Replace the R4 enumeration in the reported type with a module-local representation in `fhir-schema/src/main/java/au/csiro/pathling/definition/ElementDefinition.java`, mapping the R4 enumeration onto it in the HAPI-backed implementation.
- [x] T019 Convert the call sites in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/` to the new type, keeping dispatch typed rather than degrading to string comparison. The engine continues to read cardinality from the Spark schema in this phase.

**Checkpoint**: Definitions live below the engine and report children, cardinality and a version-independent type.

---

## Phase 3: Schema derivation

**Purpose**: Turn definitions into a Spark schema, in both modes, with the
navigable canonical structure and the structure merge that every later consumer
shares.

- [x] T028 Test schema derivation in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaBuilderTest.java` — types and cardinality taken from definitions, never from data; a repeating element is an array whether one value or many are present.
- [x] T029 Test pruning in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaPruningTest.java` — a complex element survives only where some descendant leaf is populated, so a field-less structure never arises (FR-011).
- [x] T030 Test that the dense schema is the pruned derivation with pruning skipped, asserting identical types, cardinality and field order for the branches both carry (FR-010), in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaModeParityTest.java`.
- [x] T030a Test that every derived structure orders its fields in definition order, and that a pruned structure's field order is a subsequence of the dense one (FR-057), in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaModeParityTest.java`. Field order is part of the type, so this is a correctness assertion rather than a tidiness one.
- [x] T031 Implement definition-derived schema derivation in `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaBuilder.java`.
- [x] T032 Implement pruning as a filter over the derivation output in `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaPruner.java`.
- [x] T033a Test that the schema mode, the strictness switch and the per-annotation toggles are readable from the new configuration and carry their documented defaults, in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaConfigurationTest.java`. T030 covers mode parity and T055 covers the toggles through the transform; neither covers the configuration surface itself.
- [x] T033 Add the schema mode, the strictness switch and the per-annotation toggles to a new `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaConfiguration.java`, **beside** the existing `EncodingConfiguration` rather than on it. That class lives in `encoders`, which FR-051 forbids modifying and which FR-052's carve-out does not reach, so the new options cannot go there. The nesting, extension and open-type options stay where they are and keep bounding the dense mode only (FR-044). T070 threads both onto `PathlingContext`.
- [x] T033b Classify every test class **outside `fhirpath`** that reaches the existing encoder, recording the result in `evidence/encoder-scope.md` under the category scheme already there. Split them into those that recover unaided once the public API switches and those that assert behaviour specific to the previous layout and must be converted or retired. Named starting points: `NarrowMergeTest`, `MigratedTableDecodingTest` and `ExtensionContexts` in `library-api`, the `SchemaMisalignment` helper they reach in the `encoders` test jar, and the test-jar dependencies declared by `lib/R/pom.xml`. T100d acts on the second group. Doing this in M1 sizes the flag day while there is still planning time.
- [x] T038f [P] Test the structure merge in `utilities/src/test/java/au/csiro/pathling/utilities/StructureMergeTest.java` — recursive field-wise union under a supplied canonical structure, and therefore commutative and associative regardless of merge order (FR-058). Cover **nested depth**, not one level: a merge that orders the top level correctly and the levels beneath it by discovery order passes a single-level test and is wrong. Drive it with a hand-built canonical structure so the test does not depend on `fhir-schema`.
- [x] T038g Declare the canonical structure interface in `utilities/src/main/java/au/csiro/pathling/utilities/CanonicalStructure.java` and implement the merge in `utilities/src/main/java/au/csiro/pathling/utilities/StructureMerge.java`, as the single implementation serving both reconciliation and the merging of divergent file schemas (FR-058). The interface answers two questions at any node: the canonical field order here, and the structure under a given field name. It must be **lazily navigable**, because FHIR's definition graph is cyclic — extensions are self-recursive, and a reference carries an identifier that carries a reference — so the expanded tree is infinite and the depth the merge needs is set by its operands, not known statically. Both live in `utilities`, not `fhir-schema`, because both callers must reach them and `encoders` must see only the interface, never `fhir-schema` (T009, FR-051). Pure structure mechanics over `spark-sql-api` types.
- [x] T038h [P] Test the definition-backed canonical structure in `fhir-schema/src/test/java/au/csiro/pathling/schema/DefinitionCanonicalStructureTest.java` — the field order at a node is definition order with the layout's own fields at their fixed positions, the annotations and the metadata group beside a primitive (FR-057); descent by field name reaches the structure for that element; and descending a self-recursive type to an arbitrary depth terminates without forcing an expansion beyond the depth asked for. Assert that merging two pruned structures under it yields definition order rather than discovery order. This is the input the merge cannot derive: two subsequences of a total order do not determine it, since `[id, family]` and `[id, given]` do not say which of `family` and `given` comes first.
- [x] T038i Implement the interface over the definitions in `fhir-schema/src/main/java/au/csiro/pathling/schema/DefinitionCanonicalStructure.java` (FR-057), and route `SchemaBuilder` through the same traversal. It sits beside derivation because derivation already walks the same cyclic graph and must decide the same positions; sharing the traversal is what stops the two notions of canonical order drifting apart, and structures compare positionally when they do. Memoise per FHIR type, since a merge descends the same types repeatedly.
- [x] T119 Measure merge cost over a realistic file count on object storage and record it in `evidence/merge-cost.md`, using synthetic files carrying a derived schema. The result can force a fallback for raw files (R-015). Measured here, not in US5, because it is a gate: merge cost is a property of file count and schema width, so it needs T031 and nothing else, and T118 should be designed around the answer rather than rewritten after it.

**Checkpoint**: Definitions drive schema derivation in both modes; one navigable canonical structure serves derivation, reconciliation and file merging; one merge serves every later consumer; merge cost is known.

---

## Phase 4: User Story 1 - The storage layout (Priority: P1)

**Goal**: FHIR JSON is stored in the Parquet on FHIR layout.

**Independent Test**: Load a corpus through `io` and inspect the stored schema and
values against the contract, with no FHIRPath evaluation.

*Bundle and XML ingest are deferred to M3 (T058, T068, T069), and the public API
rewiring (T070) to M4. Every annotation is deferred to M5 (T050a, T053–T055,
T063–T066), as is the primitive metadata group (T051, T061); FR-022 makes the
first safe and FR-017's carve-out bounds the second, which is what T057b and
T067a enforce.*

### Tests ⚠️ write first, confirm failing

- [ ] T050 [P] [US1] Test that a decimal is stored as its source lexical form, in `io/src/test/java/au/csiro/pathling/io/transform/DecimalTransformTest.java`. The numeric annotation beside it is T050a, in M5.
- [ ] T052 [P] [US1] Test that extensions on complex elements are stored inline, and that no field identifier and no root-level extension map are emitted, in `io/src/test/java/au/csiro/pathling/io/transform/ExtensionTransformTest.java`.
- [ ] T056 [P] [US1] Test that `contained` resources are detected and governed by the strictness switch, never silently dropped, in `io/src/test/java/au/csiro/pathling/io/transform/StrictnessTest.java`.
- [ ] T057 [P] [US1] Test that content the definition set does not describe is ignored or raises, per the switch, in the same file. Detection must compare observed keys against the definitions: the JSON reader silently skips unknown fields in every mode, so it cannot be asked to enforce this.
- [ ] T057a [P] [US1] Test that input whose cardinality contradicts the definitions — a repeating element supplied as a single object rather than a one-element array — is governed by the strictness switch and never silently coerced, in the same file. Conformant FHIR JSON always uses an array for a repeating element, so inference gets cardinality right for conformant input; this is the case where it does not, and it surfaces in the transform rather than at the read.
- [ ] T057b [P] [US1] Test that primitive id and extension content present in the input is reported through the strictness switch rather than silently dropped, in the same file. The metadata group is derived by `fhir-schema` but not populated by the transform until M5, so this content is dropped; FR-018 forbids losing it silently and FR-017's carve-out requires the loss be detectable. T078c removes this task's reason to exist.

### Implementation

- [ ] T059 [US1] Implement the JSON read and the transform into the derived schema in `io/src/main/java/au/csiro/pathling/io/transform/ResourceTransformer.java`. Read with an inferred schema, then impose types, cardinality and conventions from the definitions (R-008). This is the entry point M1, M2 and M3 are exercised through, since the public API is not rewired until M4.
- [ ] T060 [P] [US1] Implement the decimal transform in `io/src/main/java/au/csiro/pathling/io/transform/DecimalTransform.java`.
- [ ] T062 [P] [US1] Implement the extension transform in `io/src/main/java/au/csiro/pathling/io/transform/ExtensionTransform.java`.
- [ ] T067 [US1] Implement strictness checking against the definitions, including `contained` detection, in `io/src/main/java/au/csiro/pathling/io/transform/StrictnessCheck.java`.
- [ ] T067a [US1] Implement detection of primitive id and extension content in the input, reported through the strictness switch, in the same file.

**Checkpoint**: JSON is written in the new layout through `io`, without annotations and without primitive metadata. The public API is unchanged and the engine still reads the previous layout.

---

## Phase 5: User Story 2 - Lossless round trip (Priority: P1)

**Goal**: Conformant FHIR JSON round-trips to a semantically equal resource.

**Independent Test**: Round-trip a real corpus through `io`, asserting semantic
equality resource by resource, with no FHIRPath evaluation.

*The public API rewiring (T081, T082) is deferred to M4; the harness drives
`ResourceTransformer` and `ResourceSerialiser` directly, so it does not need it.
The harness also excludes primitive id and extension content, which is not
written until M5 — the exclusion is explicit and asserted, and T078c removes it.*

### Tests ⚠️ write first, confirm failing

- [ ] T071 [US2] Build the round-trip harness in `io/src/test/java/au/csiro/pathling/io/RoundTripHarness.java`, comparing semantically: object key order ignored, array order significant, numbers compared lexically.
- [ ] T072 [P] [US2] Test decimals through the harness: a trailing zero, exponent notation, a leading sign, a very small magnitude and forty significant digits, in `io/src/test/java/au/csiro/pathling/io/DecimalRoundTripTest.java`.
- [ ] T073 [P] [US2] Test that an absent element is absent from the output rather than present and null, in `io/src/test/java/au/csiro/pathling/io/EgressOmissionTest.java`.
- [ ] T074 [P] [US2] Test that a structure whose every field is null in a row is omitted rather than serialised as an empty object, in the same file. This is the default serialisation behaviour, so it will fail before the fix.
- [ ] T075 [P] [US2] Test that an array whose every element is null is omitted rather than serialised as an array of nulls, in the same file.
- [ ] T076 [US2] Run the harness over the FHIR R4 specification examples in `io/src/test/java/au/csiro/pathling/io/SpecExampleRoundTripTest.java`, excluding `Bundle` resources and asserting the exclusion is explicit rather than incidental. FR-007 means a bundle is never stored as a resource type, so a bundle can never round-trip as a bundle; its contents round-trip as the resources it is exploded into (T058, T068), which is M2's concern. The Synthea corpus in T077 is per-resource-type NDJSON and is unaffected. Primitive id and extension content is excluded on the same terms — explicitly, and asserted rather than incidental — because the metadata group is not written until M5. The R4 examples carry such content, so without the exclusion this suite fails rather than passing with a recorded gap.
- [ ] T077 [US2] Run the harness over a Synthea corpus in `io/src/test/java/au/csiro/pathling/io/SyntheaRoundTripTest.java`, carrying the same asserted exclusion for primitive id and extension content as T076.
- [ ] T078 [P] [US2] Test and thereby pin the documented limitation: resources supplied as a dataset of strings do not preserve decimal lexical form, in `io/src/test/java/au/csiro/pathling/io/StringDatasetLimitationTest.java`. Asserting it stops the limitation silently widening.
- [ ] T078a [P] [US2] Test that on a pruned schema the round-trip guarantee holds unconditionally **but for primitive id and extension content**, which FR-017's carve-out excludes until M5, and that on a dense schema content the configured nesting, extension or open-type bounds would drop is detectable rather than silently lost (FR-017), in `io/src/test/java/au/csiro/pathling/io/DenseBoundsDetectionTest.java`. The carve-out's own detectability is T057b; T078c closes it.

### Implementation

- [ ] T078b [US2] Implement detection of content the dense bounds would drop, reported through the strictness switch, in `io/src/main/java/au/csiro/pathling/io/transform/BoundsCheck.java`.
- [ ] T079 [US2] Implement egress from the layout to JSON in `io/src/main/java/au/csiro/pathling/io/egress/ResourceSerialiser.java`.
- [ ] T080 [US2] Implement omission of all-null structures and null-only arrays in `io/src/main/java/au/csiro/pathling/io/egress/EmptyPruning.java`.

**Checkpoint**: Driver 2 is demonstrated over a real corpus, with no user-visible change.

---

## Phase 6: User Story 8 - The module boundary (Priority: P3)

**Goal**: The new encoding path carries no dependency on internal Spark Catalyst
API, and the existing implementation is untouched.

**Independent Test**: The build fails when the boundary is violated.

*Mostly delivered by T004–T009; this phase verifies it. It sits at the end of M1
because there is now encoding code for T126 and T127 to inspect.*

- [ ] T125 [P] [US8] Re-check the dependency ban against real code: add a Catalyst dependency to `fhir-schema/pom.xml`, confirm the build fails, and revert. T005 proved the rule on an empty module; this proves it still holds once the module carries derivation.
- [ ] T126 [P] [US8] Re-check the import ban against real code: add a Catalyst import to the new encoding code, confirm the build fails, and revert. T007 proved the rule on an empty module.
- [ ] T127 [US8] Confirm by inspection that the new encoding path contains no expression encoder, no hand-authored serializer or deserializer expression tree and no FHIR object in a per-row plan (FR-050).
- [ ] T128 [US8] Confirm `encoders` is unmodified — `git diff main -- encoders/` is empty — and that `mvn -pl encoders` still resolves (FR-051). T128a re-checks it under a narrowed assertion once the query-time toolkit has been extended.

**Checkpoint**: Driver 5 is delivered for the encoding path and enforced by the build.

---

# Milestone 2 — The engine, layout-tolerant

The engine is rewritten to read the new layout **without losing the ability to
read the previous one**. Conversion is by addition: every traversal dispatches on
the resolved schema. The build stays green on the previous layout throughout, so
nothing here is user-visible and nothing here is a flag day.

## Phase 7: Engine foundations

**Purpose**: Everything the engine stories depend on. Blocks all of them.


### Coverage that must land before the conventions change (tests only)

*First in the milestone: these pin behaviour that Phase 8 changes, and they must
exist before the fixture mechanism moves underneath them.*

- [ ] T020 [P] Add portable JSON fixtures carrying references between resources, in the same form as the existing `viewTests` fixtures, under `fhirpath/src/test/resources/viewTests/`: a reference that resolves, one whose target is absent, one to a resource type not present, a versioned reference, and a repeating reference element with several targets.
- [ ] T021 [P] Add view test cases for `resolve()` returning the referenced resource, yielding empty on an unresolvable reference, on an absent reference and on an absent resource type, and returning all targets of a repeating reference, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T022 [P] Add cases for `resolve()` composed with traversal and with `where()`, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T023 [P] Add a case exercising resource-key production and reference-key production together with the join they feed, in `fhirpath/src/test/resources/viewTests/`. The primitives have coverage today; the join does not.
- [ ] T024 Convert the two tests using direct resource construction in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/ResolveFunctionDslTest.java` to the declarative model builder, and confirm the class no longer references the HAPI resolver factory.
- [ ] T025 Confirm every new case passes against unmodified behaviour. A failure is a defect to raise separately, not to fix here.
- [ ] T026 [P] Add a divergent-schema fixture under `fhirpath/src/test/resources/viewTests/` whose two files deliberately disagree on a leaf of a repeating element, with a view unnesting that element and projecting only that leaf.
- [ ] T027 Assert against the real engine that the divergent-schema fixture returns resources from both files (FR-036). This pins the unnesting constraint; if it fails, unnesting has been reshaped into a leaf-level read and rows are being lost silently. T027a extends it once tolerant traversal exists.

### The gate on how far the dispatch arms are built

*Moved here from Phase 13. It decides the coverage standard for the Phase 8
dispatch arms, so it cannot be settled two milestones after they are written.
Its wiring stays in Phase 13.*

- [ ] T049a **Gate.** Settle whether the source boundary refuses earlier-layout data, refuses by default but routes under the existing per-source opt-out, or routes by default. The engine reads both layouts as of Phase 8, so routing is now possible where it was not when US7 was written. The answer decides three things beyond this phase: whether FR-046 stands as written, whether Phase 8's dispatch arms are transitional scaffolding or product code deserving product-grade coverage (decision 51), and what T100e is permitted to delete. Settle it before Phase 8 opens, and therefore well before T039, which is where the answer is wired in.

### Tolerant traversal and type reconciliation

Two additions to the query-time expression toolkit, both resolving after the
input schema is known (FR-052 permits extending it; FR-048 and FR-049 are scoped
to definitions and encoding). They are foundational because both US3 and US4
depend on them, so they carry no story tag. The structure merge and canonical
ordering they build on (T038f–T038i) landed in M1.

**T038a is the risk gate for the whole approach.** If it cannot be made to pass,
the design falls back to the unbound column representation recorded in R-017,
and that is much cheaper to discover here than after US4 is built on it.
**Run it as a spike during M1**: it is the programme's single largest unknown,
it costs little, and nothing in M1 depends on the answer. T009a did so, and it
passed.

- [ ] T038m **Gate.** Spike whether the traversal expression can normalise the previous layout to the new one, rather than the engine dispatching on it. Three questions, in order. Can the expression derive the previous layout's root extension map from its own child, by walking the child's extraction chain to the resource column and emitting a struct-field access beside it? That is built only from resolved leaves, which is what `CheckAnalysis` requires of a replacement, so it should hold. Does it still hold where the child is an `UnresolvedNamedLambdaVariable`, inside the `transform` the engine wraps every repeating element in? The walk terminates at the lambda variable there, not at the resource, so the map is not self-derivable and must be supplied from the handle T094 retains — confirm whether that makes the extension branch binary, and whether a binary form still survives the analyzer on T009a's terms. And is per-level normalisation sufficient for a self-recursive type, given the expression is reapplied at every subsequent step? **On failure of the first two**, the design reverts to dispatch in the collection classes as decision 47 originally described, and T094b, T095 to T098 and T100e are rewritten against it. Run it beside T038a.
- [ ] T038a Test that the tolerant traversal expression survives the analyzer when constructed over an **unresolved** attribute — that nothing probes `dataType` before the child resolves and forces the replacement early — in `encoders/src/test/scala/au/csiro/pathling/sql/ResolveOrNullTest.scala`. This is the one residual risk of the chosen approach (R-005) and is not testable from PySpark. T009a already answered it in the affirmative as a spike, so this is the durable form of a known-passing test rather than an open question; cover the lambda-variable case too, which is the latest-resolving child the engine produces.
- [ ] T038b [P] Test that the expression resolves to a direct field reference where the input structure carries the field, and to a null of the declared fallback type where it does not, in the same file.
- [ ] T038c [P] Test the fallback types against FR-055: a singular primitive takes the definition's type, a repeating primitive an array of it, a singular complex element the bottom type, a repeating complex element an array of the bottom type. Assert that a repeating fallback survives `transform` — bare `void` does not — and that a complex fallback combines with a populated structure, which a concrete minimal structure does not. In the same file.
- [ ] T038d [P] Test that a plan using the expression prunes identically to one written with a direct field reference, by comparing `ReadSchema` from the executed plan (finding 14), in the same file. The rewrite runs before every pruning rule, so this must hold; it is the assertion that a future Spark upgrade has not reordered the optimizer batches.
- [ ] T038e Implement the tolerant traversal expression as a `RuntimeReplaceable` in `encoders/src/main/scala/au/csiro/pathling/sql/ResolveOrNull.scala`, beside the existing query-time expressions in `encoders/src/main/scala/au/csiro/pathling/encoders/Expressions.scala`, and wrap it for use from Java via `ExpressionUtils` as `ColumnFunctions.structProduct` already does. Scala, because `RuntimeReplaceable` is a Scala trait whose tree-node contract runs through `Product`; `encoders` already carries the Scala plugin and this module is where the query-time toolkit lives, so FR-052 covers it and no POM changes. No codegen: `RuntimeReplaceable` supplies `dataType`, `nullable` and a final `eval`. Note the precedent it sits beside rather than reinventing: `UnresolvedFallbackIfMissingField` in the same file already tolerates a missing field, by catching an `AnalysisException` inside `mapChildren`. T110a decides which mechanism owns which site. Follow the construction proven by the T009a spike and recorded in `evidence/t009a-analyzer-gate.md`: `UnaryLike` rather than `InheritAnalysisRules`, so the traversal target is the child; `replacement` a `lazy val` on a case class, so resolution produces a fresh copy; and a replacement built only from resolved leaves, which `CheckAnalysis` requires.
- [ ] T110a **Gate.** Settle whether the pre-existing missing-field fallback is retired once T110 lands, or retained for the repeat directive. `UnresolvedFallbackIfMissingField` in `encoders`, reached as `nullIfMissingField` and `emptyArrayIfMissingField`, already tolerates an absent field at two sites: `RepeatSelection` and the variant transform in `Collection`. T038e adds a second mechanism for the same job. The choice is not cosmetic: the existing one works by catching an `AnalysisException` inside `mapChildren`, which is exactly the analyzer-ordering fragility T038d exists to guard against. Decide before T110, and record which sites each mechanism owns.
- [ ] T038j [P] Test the reconciliation expression in `encoders/src/test/scala/au/csiro/pathling/sql/MergeCastTest.scala` — two structures of the same FHIR type with different fitted shapes project by name into the merged type and combine; fields a side lacks become null; the result is traversable. Assert it is a by-name projection and **not** a cast, by constructing two structures with the same field types in a different order and asserting they are not silently transposed.
- [ ] T038k [P] Test that folding the binary form gives the same type and values as one variadic call (finding 16), in the same file.
- [ ] T038l Implement the reconciliation expression as a variadic `RuntimeReplaceable` in `encoders/src/main/scala/au/csiro/pathling/sql/MergeCast.scala`, taking the operand being projected, the full ordered operand list so that every operand computes the same target type, and the canonical structure from T038g, which it cannot recover from the operands. It takes the **interface** declared in `utilities`, never a type from `fhir-schema`, so `encoders` gains no dependency on that module (T009, FR-051). The merge itself is T038g and the definition-backed implementation is T038i.
- [ ] T027a Extend the T026 divergent-schema fixture so the projected leaf is absent from one file's schema entirely rather than merely null, and re-assert T027 over it, so the pin exercises tolerant traversal over divergent files and not only the unnesting shape.

### Absent elements, before any fixture moves

*Moved here from Phase 9. Tolerance of an absent field is a precondition for
moving any fixture, not a consequence of the fitted schema: the existing encoder
already omits fields past its nesting bound, so absence is a property of the
current layout too, and `FhirViewExtraTest` already carries two excluded cases
for it under issue #2625. Emitting the tolerant expression at every traversal
step is what closes that gap, so it belongs ahead of the test infrastructure
rather than two phases behind it.*

*This block covers the **absence** axis only. It does not make a fixture safe to
move to the new layout, because a field present in a different shape is a
different problem, and that is Phase 8's dispatch.*

#### Tests ⚠️ write first, confirm failing

- [ ] T101 [P] [US4] Test that traversal to an element the definitions describe but the schema lacks yields an empty collection, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`.
- [ ] T102 [P] [US4] Test that traversal to an element the definitions do not describe still raises an error, in the same file.
- [ ] T103 [P] [US4] Test that selecting a choice variant absent from the schema yields empty, in the same file.
- [ ] T104 [P] [US4] Test that combining an absent element with a populated one succeeds — union, combination, conditional selection and comparison (FR-027), in the same file.
- [ ] T105 [P] [US4] Test that sibling column combination tolerates a bottom-typed complex element, in `fhirpath/src/test/java/au/csiro/pathling/projection/SiblingCombinationTest.java`. No *primitive* is untyped under FR-055, but an absent complex element is the bottom type, and the recursive selection path computes an expected element type that now meets it where it previously met a statically empty collection.
- [ ] T106 [P] [US4] Test that a view column declaring a FHIR type produces an output column of that type over an absent element (FR-028), in `fhirpath/src/test/java/au/csiro/pathling/projection/ProjectedColumnTypeTest.java`.
- [ ] T107 [P] [US4] Test that a view column declaring no type over an absent *primitive* element succeeds, carrying the type the definitions give the element (FR-030), and that deriving an output type still fails with a message naming the column, its path and the remedy for a column that genuinely carries no type information (FR-029), in the same file. An element absent from the schema must no longer reach that failure.

#### Implementation

- [ ] T110 [US4] Emit the tolerant traversal expression from T038e at **every** traversal step, replacing the direct field reference, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, with the fallback type taken from the element definition per FR-055. It must be every step, not only where absence is suspected: traversal into a fallback fails for every possible fallback type, so the tolerance has to intercept before a direct field reference is ever emitted over one.
- [ ] T111 [US4] Apply the declared FHIR type as a cast on output, not only the explicit SQL-type tag, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T112 [US4] Improve the message raised when no type information is available to name the column, its path and the remedy, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T113 [US4] Ensure sibling combination tolerates a bottom-typed complex element in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectionResult.java` and `fhirpath/src/main/java/au/csiro/pathling/projection/RepeatSelection.java`.

### Test infrastructure for the flag day

- [ ] T034 Make the JSON path with an explicit derived schema available to the YAML conformance runner's fixture factory, as the new-layout arm of the T037a dimension rather than as an unconditional switch, in `fhirpath/src/test/java/au/csiro/pathling/test/yaml/`.
- [ ] T035 Make the JSON path available to the SQL-on-FHIR view test pipeline as the new-layout arm of the T037a dimension, rather than switching it off parsing and encoding unconditionally, in `fhirpath/src/test/java/au/csiro/pathling/views/FhirViewTest.java`. The local fixtures and the submodule fixtures come along unchanged.
- [ ] T036 Make the category-C path — construct, serialise to JSON, read with the derived schema — available to the object-based test data source as the new-layout arm of the T037a dimension, so the fluent builders survive unchanged, in `fhirpath/src/test/java/au/csiro/pathling/test/`.
- [ ] T037 Add the schema mode switch to the test framework as a dimension, switchable with `-Dpathling.testSchemaMode=dense`. The default is pruned, which is safe here only because the whole absent-element block has already landed in this phase, not T110 alone: a fitted schema also needs the *typed* empty output that T107, T111 and T113 provide, or a view over an absent element crashes in sibling combination rather than yielding a column. T038 is what stops the switch failing quietly.
- [ ] T037a Add the **layout** dimension to the test framework, defaulting to the previous layout and opt-in to the new one per test, switchable with `-Dpathling.testLayout=pof`. This is a second and independent axis from T037's schema mode: a schema mode says how much of the layout is present, a layout says which conventions the fields follow. Phase 8's tests opt in as each dispatch arm lands, and T100g flips the default at the switch. Without it, T034 to T036 would move every fixture onto conventions the engine does not learn until Phase 8, and a decimal comparison would silently compare strings rather than failing loudly.
- [ ] T038 Add a test asserting the active schema mode **and the active layout** match the requested ones and failing loudly otherwise. This repository has precedent for test configuration that silently does nothing; a mode switch that fails quietly would leave a green build in a mode nobody is running.

**Checkpoint**: The join has portable coverage; the unnesting constraint is pinned; the toolkit can traverse tolerantly and reconcile shapes, and the analyzer risk is resolved either way. Traversal to an absent element yields empty everywhere and produces a typed column where a view projects one, which closes issue #2625 as well as preparing the fitted schema. Both test dimensions exist: the schema mode may now default to pruned, because the whole absent-element block precedes it, and the layout stays on the previous conventions until Phase 8 has dispatched them.

---
## Phase 8: User Story 3 - The engine reads both layouts (Priority: P1)

**Goal**: Existing expressions, views and searches return the same answers over
the new layout, **without losing the ability to read the previous one**. Every
conversion here is by addition: each traversal dispatches on the resolved schema,
so the build stays green over previous-layout data. The public API does not
switch until Phase 12.

**Independent Test**: The FHIRPath suite, both conformance baselines and the
SQL-on-FHIR compliance suite pass over data in the new layout.

*T083–T085 and T091–T093 (Coding by name) are correct under both layouts and can
land at any time, including during M1. The rest of this phase cannot: T086 to
T089 and T094 to T099 are the layout dispatch itself, and T089 runs the whole
suite over the new layout.*

*Annotations are not emitted until M5, so every value here is computed rather
than read from an annotation. That is what FR-022 requires in any case; the
annotation fast paths are T086a, T087a, T095a and T096a, in M5.*

*The dispatching expression must satisfy one rule, and it now holds without
qualification: **every branch yields the same `dataType`**, across layouts as
well as within one, because T094b normalises the previous layout to the new
layout's shape rather than carrying it forward. That is what keeps T108 and
FR-054 true. Decision 47 originally had to weaken this across layouts; decision
55 records why it no longer does.*

### Tests ⚠️ write first, confirm failing

- [ ] T083 [P] [US3] Test that a full canonical Coding structure decodes as it does today, that a `{code, system}` structure decodes with the remaining properties null, and that reordered fields follow names rather than positions, in `terminology/src/test/java/au/csiro/pathling/fhirpath/encoding/CodingSchemaTest.java`.
- [ ] T084 [P] [US3] Test that a structure carrying no recognisable Coding field is rejected with an error naming the expected and the actual fields, in the same file.
- [ ] T085 [P] [US3] Test each terminology operation against a narrowed Coding column, in `terminology/src/test/java/au/csiro/pathling/sql/udf/NarrowCodingTest.java`.
- [ ] T086 [P] [US3] Test that decimal comparison, arithmetic and ordering over lexically stored decimals match the current results, and that the same operations over a previous-layout `DECIMAL(32,6)` column continue to match them, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/DecimalCollectionTest.java`. The numeric annotation as a fast path is T086a, in M5.
- [ ] T087 [P] [US3] Test that cross-unit quantity comparison computes canonicalisation when no annotation is present, and that it continues to work over a previous-layout quantity carrying `_value_canonicalized` and `_code_canonicalized`, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/QuantityCollectionTest.java`. The `_canonical_exact` annotation as a fast path is T087a, in M5.
- [ ] T088 [P] [US3] Test that `resolve()` works without a stored versioned-key column, reusing the fixtures from T020–T023, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/ResolveFunctionDslTest.java`.
- [ ] T089a [P] [US3] Test that the traversal expression normalises each previous-layout discriminator to the new-layout shape, in `encoders/src/test/scala/au/csiro/pathling/sql/ResolveOrNullTest.scala`: a `DECIMAL(32,6)` value with its `_scale` companion, a quantity carrying `_value_canonicalized` and `_code_canonicalized`, an element carrying `_fid` against the resource's root map, and a stored versioned key. Assert the output type is the new-layout type in every case and **equal to the type the same traversal yields over new-layout input**, which is the property that lets decision 47 drop its cross-layout carve-out. Cover the lambda case T038m settles.
- [ ] T089 [US3] Test that the full suite passes over files written with no annotations (FR-022, SC-004), in `fhirpath/src/test/java/au/csiro/pathling/test/AnnotationFreeSuiteTest.java`. Until M5 this is the only mode the layout is written in, so this asserts the ordinary path rather than an edge case; the annotated case is covered per kind in M5, and T090 asserts the choice is made from the schema.

### Implementation

- [ ] T091 [US3] Resolve Coding fields by name against the schema of the row being decoded, resolved once per schema rather than per row, in `terminology/src/main/java/au/csiro/pathling/fhirpath/encoding/CodingSchema.java`. Absent fields decode as null.
- [ ] T092 [US3] Route the terminology helpers through the resolver in `terminology/src/main/java/au/csiro/pathling/sql/udf/TerminologyUdfHelpers.java`.
- [ ] T093 [P] [US3] Remove index-based assumptions from `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/CodingCollection.java`, `fhirpath/src/main/java/au/csiro/pathling/fhirpath/FhirPathType.java` and `library-api/src/main/java/au/csiro/pathling/library/TerminologyHelpers.java`.
- [ ] T094 [US3] Implement sibling resolution — resolve a named sibling of a primitive within its parent structure — in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, retaining the parent handle and element name on traversal to a primitive. This one mechanism serves three consumers: annotations, primitive metadata (R-014), and the previous layout's root extension map (T094a).
- [ ] T094a [US3] Remove `extensionMapColumn` from the collection hierarchy under `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/`, deriving the root extension map at the traversal site from the handle T094 retains. The parameter currently threads through fourteen classes for the one reason `ResourceCollection` states in its javadoc — preserving the resource-level map across copies with a different column representation — which T094 makes unnecessary. Layout-independent, so it can land as soon as T094 does. Recorded as decision 48.
- [ ] T094b [US3] Implement previous-layout normalisation as branches of the traversal expression in `encoders/src/main/scala/au/csiro/pathling/sql/ResolveOrNull.scala`, chosen from the resolved schema once per schema and never per row. This is the single site at which the two layouts meet: above it the engine sees only the new layout, which is what lets T095 to T098 be written once rather than as dual paths, and what reduces T100e to deleting these branches. The four discriminators are the ones decision 47 lists. Normalisation is per level rather than per subtree, which is sufficient because T110 reapplies the expression at every subsequent step; that matters for extensions, whose type is self-recursive and whose expansion is therefore infinite.
- [ ] T095 [US3] Decode decimals from the lexical representation, keeping query-time precision unchanged (FR-035), in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/DecimalCollection.java`. Written against the new layout only: T094b normalises a previous-layout `DECIMAL(32,6)` column with its `_scale` companion before it reaches here, so this class carries no second path. The computed path is the primary implementation, not a fallback, since nothing annotated exists until M5. The fast path is T095a.
- [ ] T096 [US3] Move quantity handling to the FHIR structure, computing canonicalisation from the structure itself, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/encoding/QuantityEncoding.java` and `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/QuantityCollection.java`. Reading the `_canonical_exact` annotation in preference to the specification's narrower `_canonical` is T096a, in M5.
- [ ] T097 [US3] Implement inline extension traversal in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/Collection.java` and `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/ResourceCollection.java`, against the new layout only. The previous layout's root map keyed by `_fid` is normalised to the inline shape by T094b before it reaches here, so there is no second branch in the engine. This is what makes decision 48's claim literally true rather than nearly so.
- [ ] T098 [US3] Compute reference keys from the conformant identifier elements rather than a stored versioned-key column, in the reference resolution and join machinery under `fhirpath/src/main/java/au/csiro/pathling/fhirpath/`. The computed path is primary; FR-033 leaves a precomputed key to be added as an annotation if T136 shows joins need it.
- [ ] T099 [P] [US3] Replace the canonicalised-quantity field-name constants in `fhirpath/src/main/java/au/csiro/pathling/search/filter/FhirFieldNames.java` and update the search matchers that use them.

**Checkpoint**: The engine reads the new layout over the existing fixtures **and still reads the previous one**. The public API has not switched yet, and the build is green.

---

## Phase 9: User Story 4, continued - Shape reconciliation (Priority: P1)

**Goal**: Collections of one FHIR type whose fitted schemas differ combine and
stay traversable, and shape-sensitive assertions pin the schema they ran against.

**Independent Test**: Reach the same FHIR type by two paths whose fitted schemas
differ; assert the combination holds every element of both.

*The absent-element work that used to sit here — T101 to T107 and T110 to T113 —
moved to Phase 7, because tolerance of an absent field has to precede any fixture
movement. What remains is the reconciliation half, which depends on T038l rather
than on T110.*

### Tests ⚠️ write first, confirm failing

- [ ] T104a [P] [US4] Test that two collections of the same FHIR type reached by different paths, whose fitted schemas differ, combine successfully — holding every element of both and remaining traversable (FR-056) — and that the result's fields are in definition order (FR-057), in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/operator/ShapeReconciliationTest.java`. Cover `combine`, `|`, equality of complex values, conditional selection and membership.
- [ ] T109 [US4] Run the shape-sensitive tests with an explicit expected schema assertion, since a pruned schema is derived from the whole fixture set and adding a fixture can silently flip an assertion from a null branch to a missing-field branch.

### Implementation

- [ ] T113a [US4] Apply the reconciliation expression from T038l where operands are prepared for combination, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/CombiningLogic.java`, generalising what `prepareArray` already does for `DecimalCollection` (FR-056). The operators themselves do not change, and the FHIR-type promotion in `FhirPathBinaryOperator.reconcileTypes` stays a separate, definition-driven step ahead of it.
- [ ] T113b [US4] Apply reconciliation at the remaining sites that need two operands to share a SQL type — conditional selection and membership — and at `ColumnRepresentation.traverseChoice`, which coalesces across several variant fields at once and therefore passes the whole ordered operand list rather than folding (FR-056).

**Checkpoint**: Two fitted shapes of one FHIR type reconcile, so the public API
can safely write a schema fitted to the data.

## Phase 10: User Story 5 - Divergent files read as one dataset (Priority: P2)

**Goal**: Batches written at different times query as one dataset without loss.

**Independent Test**: Write two batches with divergent schemas; query across both
and assert every resource appears with correct cardinality.

*In M2 rather than M1 because its subject is batches arriving over time, and
because T116 depends on US4. It sits **before** the flip rather than after it:
divergent fitted schemas arise the moment the new layout is written, so reading
mixed-shape files has to work by Phase 12, not after. Until then the batches are
written through `io`'s own entry points rather than the public write path. The
merge it needs (T038g) landed in M1.*

### Tests ⚠️ write first, confirm failing

- [ ] T114 [P] [US5] Test that reading files with differing schemas returns the union of their elements with no resource lost, in `library-api/src/test/java/au/csiro/pathling/library/io/source/SchemaMergingTest.java`.
- [ ] T115 [P] [US5] Test that appending a batch whose schema differs from the table's succeeds and widens the table, in `library-api/src/test/java/au/csiro/pathling/library/io/sink/DeltaSchemaMergingTest.java`.
- [ ] T116 [US5] Test that a view unnesting a repeating element and projecting a leaf only one batch populated returns resources from both, extending T026–T027 to the source and sink level, in `library-api/src/test/java/au/csiro/pathling/library/io/source/DivergentFileViewTest.java`.

### Implementation

- [ ] T117 [P] [US5] Enable schema merging on append in `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`.
- [ ] T117a [US5] Enable schema auto-merge on the upsert path in `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java`, so a divergent source widens the target as an append now does. This **reverses a deliberate guarantee**: the upsert path today refuses to widen, so that tolerance does not become schema evolution the caller did not ask for. That reasoning assumed a schema derived from encoder configuration and stable between batches. A fitted schema comes from the data, so divergence is the steady state and the refusal would fire on ordinary use. Append and upsert must not give two answers to the same question. Recorded as decision 41; `NarrowMergeTest` is rewritten by T100d.
- [ ] T118 [P] [US5] Enable schema merging on read in `library-api/src/main/java/au/csiro/pathling/library/io/source/ParquetSource.java`. **The opt-out takes a supplied schema, not a boolean**, per the T119 gate: `mergeSchema=false` was measured returning as few as 6 of 24 leaf columns on a divergent corpus, silently, so a boolean opt-out is a trap rather than a choice. A supplied schema is safe only while it covers the union of the files, so the mechanism is a way to stop paying for the merge on every read, not a way to never pay for it — the cheap path is to merge once and persist the result, or take the writer's schema, and supply that.
- [ ] T118b [P] [US5] Resolve schemas lazily and per resource type in `library-api/src/main/java/au/csiro/pathling/library/io/source/FileSource.java`, whose eager `buildResourceMap` multiplies the per-type cost by the number of types in the warehouse before a single query runs. The T119 gate names this as the larger lever on any storage, and the one obtainable without knowing the object-storage constant.
- [ ] T118c [P] [US5] Expose the supplied-schema opt-out in `lib/python/pathling/datasource.py` and `lib/R/R/datasource.R`, as T049 does for the detection opt-out. The API contract lists every added option as surfaced in Java, Python and R.
- [ ] T118a [US5] Where a merged schema is produced rather than obtained from Spark, produce it with the shared merge from T038g so field order stays canonical (FR-057, FR-058). A merge that appends newly discovered fields in discovery order rather than definition order yields structures that cannot be combined and, worse, compare positionally.

**Checkpoint**: Incremental loading works with schemas fitted to data.

---

---

# Milestone 3 — Ingest formats

The remaining ingest formats reach the new layout. Still nothing user-visible.

## Phase 11: Remaining ingest formats (US1)

- [ ] T058 [P] [US1] Test that a bundle is exploded to per-type tables and is never stored as a resource type, in `io/src/test/java/au/csiro/pathling/io/transform/BundleTransformTest.java`.
- [ ] T058a [P] [US1] Test that XML ingest reaches the same stored result as the equivalent JSON, in `io/src/test/java/au/csiro/pathling/io/transform/XmlIngestTest.java`. Cover a primitive extension, which XML carries as a child element rather than an underscore-prefixed sibling, and a repeating element occurring once. Spark's native XML reader would infer that last case as a non-array, but T069 does not use it — cardinality comes from the FHIR parser and the definitions — so this asserts the chosen design actually avoids the hazard.
- [ ] T068 [US1] Implement bundle explosion and reference resolution within a bundle in `io/src/main/java/au/csiro/pathling/io/transform/BundleTransformer.java`.
- [ ] T069 [US1] Implement XML ingest by parsing with FHIR tooling in a UDF and handing JSON to the transform, in `io/src/main/java/au/csiro/pathling/io/transform/XmlIngest.java`.
- [ ] T069a [US1] Run the M1 round-trip harness over the resources a bundle is exploded into, and over XML input, in `io/src/test/java/au/csiro/pathling/io/IngestFormatRoundTripTest.java`. T076 excludes bundles permanently and correctly, since FR-007 means a bundle is never stored, and T077 runs over per-resource-type NDJSON — so without this task nothing round-trips what this phase adds.

**Checkpoint**: Every ingest format the previous encoder accepted reaches the new layout, and each has been round-tripped through the M1 harness.

---

# Milestone 4 — The flip

The flag day, and the only breaking release. The public API switches to the new
layout, earlier layouts are detected at the source boundary, and the layout is
documented. The engine already reads both layouts, so the build does not go red.

## Phase 12: The public API switch
**Goal**: `PathlingContext` and `NdjsonSink` write and read the new layout.

*The point of no return, and the only breaking release. It lands after US4
because until absent elements behave the public API would be writing a fitted
schema the engine cannot fully query, and beside US7 because a user whose data
predates the flip must meet a message rather than a wrong answer. The engine
already reads both layouts, so the build does not go red across the switch.*

*T108 and T108a sit here rather than with the rest of US4: they assert the
expression-to-column guarantee through the public API, which has no fitted mode
until this phase. The semantics they rest on are covered in Phase 9 by T101–T107
and T104a, so FR-054 and SC-010 are proven end to end here and not before.*

*The layout lands annotation-free and without primitive metadata. FR-022 makes
the first safe; FR-017's carve-out, asserted by T057b, bounds the second. Both
close in M5.*

- [ ] T070 [US1] Wire the transform into `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signatures (FR-043).
- [ ] T081 [US2] Wire egress into the decode entry point in `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signature.
- [ ] T082 [P] [US2] Wire egress into `library-api/src/main/java/au/csiro/pathling/library/io/sink/NdjsonSink.java`.
- [ ] T108 [P] [US4] Test that one column expression, built with no reference to any dataset, applied to a fitted dataset and to a dense dataset holding the same resources, yields equal results for every expression in the engine's expression-level test set (FR-054, SC-010), in `library-api/src/test/java/au/csiro/pathling/library/FhirPathToColumnTest.java`. This is the assertion that the expression-to-column API's unchanged signature is honest.
- [ ] T108a [P] [US4] Test that a column over an absent primitive element reaching a caller through the expression-to-column API carries the element's type and can be written to Parquet (FR-030), in the same file.
- [ ] T100 [US3] Run the full FHIRPath suite, both YAML conformance baselines and the SQL-on-FHIR compliance suite over the new layout (SC-002); remove exclusion entries that have become obsolete rather than leaving them to self-report (SC-003). Among them are the two `FhirViewExtraTest` exclusions for issue #2625, which T110 closes. Confirm the engine reads the new layout on every path; it still retains the previous-layout reader, which FR-053 permits until T100e removes it in M6.
- [ ] T100g Flip the T037a layout dimension's default to the new layout. The previous-layout arm stays available, because the dispatch arms it exercises survive until T100e and decision 51 leaves their coverage standard to T049a.
- [ ] T100b Rebuild `library-runtime`, clear both `cache` and `jars` across every `~/.ivy2*` tree and recreate the directories, then run the Python and R suites over the new layout. Assert that encode, decode, the file sources and sinks and the detection opt-out behave as before from both bindings (SC-008). The clearing step is not optional: the SNAPSHOT filename never changes, so without it Ivy reuses a stale jar and both suites pass without exercising any of this work.
- [ ] T100c Remove the encode and decode plumbing in `library-api/src/main/java/au/csiro/pathling/library/` left unreachable by T070 and T081 — the three encode paths over map partitions, the decode path and the resource parser — confirming first that nothing outside the module references them. The `encoders` module itself is untouched and keeps its coordinates, so migration tooling is unaffected; FR-051 is about that module, not this plumbing.
- [ ] T100d Convert or retire the test classes T033b classified as asserting behaviour specific to the previous layout. `NarrowMergeTest` is rewritten against the widening behaviour T117a introduces rather than the refusal it asserts today.
- [ ] T128a [US8] Re-check the `encoders` boundary under the narrowed assertion: the HAPI bridge and the pre-existing expressions are unchanged, and the module has gained only `ResolveOrNull` and `MergeCast`, which FR-052 permits. Confirm `mvn -pl encoders` still resolves.

**Checkpoint**: The engine reads the new layout and the public API writes it. The old encoder is no longer read by the engine, and both language bindings are verified against the switch.

---

## Phase 13: User Story 7 - Detection of earlier layouts (Priority: P2)

**Goal**: Reading data written by an earlier release fails with an actionable
message rather than producing wrong answers.

**Independent Test**: Read previous-layout data through each file-based source
and assert the error.

*Immediately before the public API switch, and not earlier. The detector rejects
the layout an earlier release wrote, which is the only layout anyone holds until
the flip. Shipping it in M1 would reject every existing user's data in a release
whose engine still reads that data exactly as intended. It must be in place by
Phase 12 and must not ship before it.*

*It depends only on Setup, so it can be built at any point; it must not be wired
in before Phase 12.*

*The gate this phase is built to, T049a, is settled in Phase 7. Its answer says
whether the boundary refuses or routes, and therefore what T039 to T048 assert.*

### Tests ⚠️ write first, confirm failing

- [ ] T039 [P] [US7] Test the layout detector against a full previous-layout schema, a sparse previous-layout schema, a new-layout schema and a schema with no markers, in `library-api/src/test/java/au/csiro/pathling/library/io/source/LayoutDetectorTest.java`.
- [ ] T039a [P] [US7] Test the detector against a **mixed** schema — one table carrying marker fields from both layouts, as an append into a previous-layout table produces — in the same file. Decision 47 assumes two layouts never meet inside one query; this is the case that would falsify it, and none of T039's four schemas covers it.
- [ ] T040 [P] [US7] Test that detection walks nested structures and arrays of structures, not only top-level fields, in the same file.
- [ ] T041 [P] [US7] Test that reading conforming data succeeds unchanged, that a sparse but conforming schema succeeds, and that an unclassifiable schema succeeds, in `library-api/src/test/java/au/csiro/pathling/library/io/source/FileSourceDetectionTest.java`.
- [ ] T042 [P] [US7] Test that an unsupported layout fails at read time with a message naming the resource type, the detected layout, the expected layout and the remedy, asserting the exact message content so it cannot silently degrade, in the same file. **The remedy is re-import from source**, and the message says so plainly, because no in-place rewrite exists in this release: migration of data at rest is out of scope here, and the previous layout stored decimals as a fixed-precision numeric, so any rewrite would be lossy where a re-import is not. T134a records the rewrite tool as future work.
- [ ] T042a [P] [US7] Test that **writing** new-layout data into a table holding an earlier layout is detected and refused with the same actionable message, rather than failing with a storage-layer schema error or silently widening the table, in `library-api/src/test/java/au/csiro/pathling/library/io/sink/SinkDetectionTest.java`. FR-037 and SC-006 name sources only, and T117 and T117a turn auto-merge on, so an append of a resource type carrying no conflicting types would otherwise succeed and produce the hybrid table T039a describes.
- [ ] T043 [P] [US7] Test that the opt-out permits the read and is scoped to one source, in the same file.
- [ ] T044 [P] [US7] Test that detection introduces no additional file access beyond the schema metadata the read already obtains (FR-038), in the same file.

### Implementation

- [ ] T045 [US7] Implement the layout detector, classifying a schema by marker fields and returning the markers found so the message can cite evidence, in `library-api/src/main/java/au/csiro/pathling/library/io/source/LayoutDetector.java`.
- [ ] T046 [US7] Apply the check in `library-api/src/main/java/au/csiro/pathling/library/io/source/FileSource.java`, so `ParquetSource` and `DeltaSource` inherit it.
- [ ] T047 [US7] Apply the check per table as each resource type is resolved in `library-api/src/main/java/au/csiro/pathling/library/io/source/CatalogSource.java`.
- [ ] T046a [US7] Apply the check on the write path in `library-api/src/main/java/au/csiro/pathling/library/io/sink/DeltaSink.java` and the other file-based sinks, classifying the **target**'s existing schema before a merge is attempted, so the refusal precedes the widening.
- [ ] T048 [US7] Thread the opt-out through the source constructors in `library-api/src/main/java/au/csiro/pathling/library/io/source/`.
- [ ] T049 [P] [US7] Expose the opt-out in `lib/python/pathling/datasource.py` and `lib/R/R/datasource.R`.

**Checkpoint**: Old data is rejected with an actionable message everywhere it can enter.

---

---

## Phase 14: Documentation and measurement

**Goal**: The layout users now receive is documented, and its cost is recorded.

*These sit with the flip rather than after it. T129 and T130 describe the layout
the public API has just started writing; landing them later would ship an
undocumented layout. T136 measures the flip itself, and because the layout is
annotation-free at this point it measures the computed paths at their most
expensive — which is what makes it the input that orders M5.*

- [ ] T129 [P] Rewrite `site/docs/libraries/io/schema.md` from [contracts/storage-layout.md](contracts/storage-layout.md), including the fitted-schema statement, the unnesting caution for direct SQL consumers and the compatibility statement for existing files.
- [ ] T130 [P] Document the new options and the behaviour changes from [contracts/library-api.md](contracts/library-api.md) in `site/docs/libraries/`, including the decimal limitation on the string-dataset path, that a column expression remains valid over any conformant schema with absent primitives carrying their definition-derived type, and that a column over an absent *complex* element is bottom-typed and so cannot be written to Parquet and is omitted in JSON.
- [ ] T136 Re-run the split benchmark and compare against the T003 baseline. Record encode, decode, planning and execution separately. No outcome is required (SC-007). Because nothing is annotated yet, this measures every annotated operation on its computed path, so it is what orders Phase 15: FR-033 in particular leaves a precomputed reference key to be added if joins prove to need one. Re-run after each annotation kind lands.

**Checkpoint**: The new layout is documented and its unaccelerated cost is on record.

---

# Milestone 5 — The gaps

The two gaps the flip deliberately left open: annotations, then primitive ids and
extensions. Each annotation kind is an independent increment.

## Phase 15: Annotations

**Goal**: Each annotation kind is emitted by the encoder and used by the engine
as a fast path, one kind at a time.

*Ordered by what T136 measured, not by this list. Every kind is independent: it
can ship on its own, and until it ships the engine computes the value instead.
FR-021 is satisfied when the last kind lands, and FR-023 with T090.*

*Correctness is never at stake here. FR-022 makes an annotation a fast path and
nothing more, which is what Phase 8 already delivers and T089 already asserts.*

### Tests ⚠️ write first, confirm failing

- [ ] T050a [P] Test that a decimal carries the numeric annotation beside its lexical form, in `io/src/test/java/au/csiro/pathling/io/transform/DecimalTransformTest.java`. Split from T050, which pins the lexical form in M1.
- [ ] T053 [P] [US1] Test that dates carry range annotations reflecting the stated precision, in `io/src/test/java/au/csiro/pathling/io/annotation/DateRangeAnnotationTest.java`.
- [ ] T054 [P] [US1] Test that a quantity carries both canonical annotations, in `io/src/test/java/au/csiro/pathling/io/annotation/QuantityCanonicalAnnotationTest.java`: `__<field>_canonical` present at the specification's `DECIMAL(38,6)`, and `__<field>_canonical_exact` immediately after it, asserting on the second that quantities differing by orders of magnitude do not compare equal. Pin the order, since field order is part of the type (FR-057).
- [ ] T055 [P] [US1] Test that each annotation can be disabled individually and that disabling one does not affect the others, in `io/src/test/java/au/csiro/pathling/io/annotation/AnnotationToggleTest.java`.
- [ ] T086a [P] Test that decimal comparison, arithmetic and ordering take the numeric annotation as a fast path where the schema carries one, giving results equal to the computed path T086 pins, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/DecimalCollectionTest.java`.
- [ ] T087a [P] Test that cross-unit quantity comparison takes the `_canonical_exact` annotation as a fast path where present, giving results equal to the computed path T087 pins, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/QuantityCollectionTest.java`.
- [ ] T090 Test that whether an annotation is used is decided from the schema rather than per row (FR-023), by asserting the generated plan differs between an annotated and an unannotated input, in `fhirpath/src/test/java/au/csiro/pathling/test/AnnotationFreeSuiteTest.java`.

### Implementation

- [ ] T063 [US1] Implement the annotation processor registry in `io/src/main/java/au/csiro/pathling/io/annotation/AnnotationProcessors.java`, with each processor individually enableable.
- [ ] T064 [P] [US1] Implement the numeric annotation in `io/src/main/java/au/csiro/pathling/io/annotation/NumericAnnotation.java`.
- [ ] T065 [P] [US1] Implement the date range annotation in `io/src/main/java/au/csiro/pathling/io/annotation/DateRangeAnnotation.java`.
- [ ] T066 [P] [US1] Implement both quantity canonical annotations in `io/src/main/java/au/csiro/pathling/io/annotation/QuantityCanonicalAnnotation.java`: the specification's `__<field>_canonical` at its `DECIMAL(38,6)`, and `__<field>_canonical_exact` carrying the wider arbitrary-scale representation, in that order. The `_exact` form MUST carry the canonicalised unit code alongside the value — the previous layout used two fields for this, `_value_canonicalized` and `_code_canonicalized`, and a value without its base unit makes one metre and one second compare equal. `fhir-schema` fixes the names and the positions; the Spark types of both are settled here.
- [ ] T095a Take the numeric annotation as a fast path in decimal decoding, chosen from the schema and never per row, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/DecimalCollection.java`. The lexical path T095 builds stays the fallback and stays correct.
- [ ] T096a Take the `_canonical_exact` annotation in preference to the specification's narrower `_canonical` in quantity handling, computing canonicalisation only where neither is present, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/encoding/QuantityEncoding.java` and `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/QuantityCollection.java`.

**Checkpoint**: FR-021 and FR-023 are satisfied, and every annotated operation has both a fast path and a computed path that agree.

---

## Phase 16: User Story 6 - Primitive ids and extensions (Priority: P2)

**Goal**: A primitive element's id and extensions are stored, and expressions can
navigate to them.

**Independent Test**: Round-trip a resource carrying primitive metadata, then
evaluate expressions reaching it.

*Both halves land here, past the flip. The engine cannot navigate primitive
metadata today and the previous layout cannot represent it at all, so nothing
regresses by deferring the reading. Deferring the writing is the deliberate cost
recorded in FR-017's carve-out and decision 49: until T078c, a warehouse written
by this release does not carry primitive id or extension content, and T057b is
what keeps that loss visible rather than silent.*

### Tests ⚠️ write first, confirm failing

- [ ] T051 [P] [US1] Test that a primitive element's id and extensions are stored in the metadata group beside it, in `io/src/test/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransformTest.java`.
- [ ] T078c [US6] Re-assert FR-017 unconditionally: remove the primitive-metadata exclusion from the round-trip harness and from T076, T077 and T078a, and delete the carve-out from the spec. This is what closes decision 49.

- [ ] T120 [P] [US6] Test navigation to a primitive element's extensions, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/PrimitiveMetadataNavigationTest.java`.
- [ ] T121 [P] [US6] Test navigation to a primitive element's id, in the same file.
- [ ] T122 [P] [US6] Test that both yield empty where the source carried neither, in the same file.
- [ ] T123 [P] [US6] Add fixtures carrying primitive ids and extensions under `fhirpath/src/test/resources/viewTests/`.

### Implementation

- [ ] T061 [P] [US1] Implement the primitive metadata transform in `io/src/main/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransform.java`.
- [ ] T124 [US6] Implement navigation to the metadata group through the sibling resolution added in T094, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/Collection.java`.

**Checkpoint**: Driver 3 is delivered end to end — stored and readable — and the round trip is unconditional again.

---

# Milestone 6 — Completion

The previous-layout reader is removed, the superseded design is retired, and the
follow-ups are raised. Nothing user-visible.

## Phase 17: Completion

*T131, T132, T133, T134, T134a, T137 and T138 depend on nothing beyond M1 and may
land at any point; T131 in particular documents modules that already exist.
Documentation of the layout itself is not here — T129 and T130 ship with the flip
in Phase 14, because a layout users hold and cannot read about is worse than one
they do not hold yet.*

*T100e is the only task here that removes behaviour, and its scope is set by
T049a: if the source boundary routes earlier-layout data rather than refusing it,
the previous-layout reader is a product feature and removing it is a breaking
change rather than a cleanup.*

*No migration tooling is built by this programme. T042's remedy is re-import from
source, T134a raises the rewrite tool as a follow-up, and FR-051's retention of
`encoders` is what keeps it possible later.*

### The gate on the end state

- [ ] T100f **Gate.** Settle FR-053's end-state wording before T100e runs. The provisional position is that FR-053 states the transitional permission explicitly — the engine MUST read the new layout and MAY retain a reader for the previous one until this phase, after which it reads only the new layout. Confirm or revise it, and reconcile it with whatever T049a settled.

### Tasks

- [ ] T131 [P] Update the module list and dependency diagram in `CONTRIBUTING.md`, and the build-order line in `.claude/CLAUDE.md`. May land as soon as M1 does; `fhir-schema`, `io` and the `utilities` additions already exist by then.
- [ ] T132 [P] File the upstream Spark issue as a follow-up to SPARK-48148, with the reproduction from `evidence/spark-type-findings.md`: exact string parsing covers only byte-array and positionally-readable content references, so the string-dataset and `from_json` paths still route numbers through a double.
- [ ] T133 [P] Raise the follow-up issue recording the two alternative bindings from R-017 as future options — evaluation-time schema binding, and an unbound column representation bound when the dataset is known. Note what each would add beyond what is built here: bind-time validation and a required-schema manifest, and therefore Pathling-level messages naming the missing element instead of a Spark analysis failure. Note also that they remain compatible, because the representation of absence and the reconciliation mechanism are shared. Typed absent elements are **no longer** a follow-up: FR-055 delivers them for primitives.
- [ ] T134 [P] Raise the follow-up issue for the decimal quoting pre-pass (R-009), recording that a blanket approach fails because a quoted number against an integer target is rejected.
- [ ] T134a [P] Raise the follow-up issue for the data-at-rest rewrite tool that T042's message says does not yet exist. Record that the retained `encoders` module is what keeps it possible, that a rewrite cannot restore decimal lexical form because the previous layout stored a fixed-precision numeric, and that a re-imported warehouse therefore carries the losslessness guarantee where a rewritten one does not. This closes the gap between the Constitution Check's retention rationale and work that appears nowhere in this programme.
- [ ] T134b [P] Raise the follow-up issue tracking the server's move onto the post-change line. The assumptions in `spec.md` pin the server to the last library release before Phase 12 and require it to reach the post-change line before the pinned line stops receiving fixes. That obligation has a deadline and no owner anywhere in this programme, which is what this task fixes.
- [ ] T134c [P] Raise the follow-up issue for the object-storage half of the T119 merge gate. R-015's condition was *narrowed*, not discharged: `evidence/merge-cost.md` establishes the shape of the curve locally and shows Delta does not bite, but no object-storage harness exists in this repository, so the wall clock a user on S3 would see for raw files is still unmeasured. Record that the fallback R-015 contemplates therefore remains open rather than ruled out.
- [ ] T135 Benchmark the two ingest mechanisms against each other — the chosen transform approach and direct parsing into a variant — and record the comparison in `evidence/ingest-comparison.md`. This records evidence for a future release rather than deciding anything here: the mechanism is settled by this point, so the lexical-decimal limitation stands for this programme, pinned by T078 and documented per FR-020 (R-009).
- [ ] T137 Correct or retire `openspec/parquet-on-fhir-design.md`, which this specification supersedes: the stored decimal precision is wrong, the nested-pruning risk does not apply to the engine and its proposed mitigation was insufficient, and the first argument against schema inference does not hold for FHIR JSON.
- [ ] T138 Remove or archive the five superseded change directories under `openspec/changes/` belonging to this programme.
- [ ] T100e Remove the previous-layout normalisation branches from the traversal expression (T094b), so the engine reads only the new layout. One site, not six: because the two layouts meet only inside that expression, nothing in `DecimalCollection`, `QuantityCollection`, `QuantityEncoding`, `Collection`, `ResourceCollection` or the reference machinery carries a second path to unpick. Scope is set by T049a and the wording by T100f. This is what finally makes FR-053 true, and it is why decision 51 records the dispatch arms as a deliberate trade rather than an oversight.
- [ ] T139 Run every scenario in [quickstart.md](quickstart.md) as final validation.

---

## Traceability

Every functional requirement in [spec.md](spec.md) and the task or tasks that
deliver it. Because identifiers are never renumbered, moving a task between
phases leaves these rows untouched — except where a row names the milestone, and
those labels are updated on a move. Adding a task always updates its row, so a
row is revised whenever a suffixed task is introduced.

Where a requirement is delivered across more than one milestone the row says so.
Six are staged into M5 by design: FR-002 and FR-003 in part, and FR-004, FR-005,
FR-021 and FR-023 entirely. Checked mechanically as part of the
consistency pass, which expands every range against the task list rather than
trusting the numbering.

| Requirement | Tasks |
| --- | --- |
| FR-001 Layout conformance | T050, T052, T056, T057, T057a, T057b, T059, T060, T062, T067, T067a (M1), T058, T058a, T068, T069, T069a (M3), T070 (M4), T051, T053–T055, T061, T063–T066 (M5) |
| FR-002 Decimals lexical plus annotation | T050, T060 (M1), T050a, T064 (M5) |
| FR-003 Primitive metadata groups, inline extensions, no identifier or map | T052, T062 (M1), T051, T061 (M5) |
| FR-004 Date range annotations | T053, T065 (M5) |
| FR-005 Both quantity canonical annotations, the specification's and a magnitude-preserving one | T054, T066 (M5) |
| FR-006 `contained` excluded and detected | T056, T067 |
| FR-007 `Bundle` never stored | T058, T068 |
| FR-008 Schema derived from definitions | T028, T031 |
| FR-009 Pruned and dense modes | T029, T032, T033, T033a |
| FR-010 Dense is the same derivation unpruned | T030 |
| FR-011 No field-less structure | T029 |
| FR-012 Types and cardinality never from data | T028 |
| FR-013 Child enumeration | T013, T016 |
| FR-014 Cardinality exposed | T014, T017 |
| FR-015 Version-independent type representation | T015, T018 |
| FR-016 Round-trip equality | T071, T076, T077 |
| FR-017 Unconditional on pruned, bounded and detectable on dense | T057b, T067a, T078a, T078b (M1, under the primitive-metadata carve-out), T078c (M5, closing it) |
| FR-018 Strictness switch | T057, T067 |
| FR-019 Absent elements omitted on export | T073, T074, T075, T080 |
| FR-020 Limitations documented per path | T078, T130 |
| FR-021 Annotations default on, individually disableable | T055, T063 (M5) |
| FR-022 Engine computes when annotations absent | T089 |
| FR-023 Fast path chosen from the schema | T090 (M5) |
| FR-024 Absent element yields empty | T101, T110, T110a |
| FR-025 Undefined element raises | T102 |
| FR-026 Absent choice variant yields empty | T103 |
| FR-027 Absent combines with populated | T104, T105, T113 |
| FR-028 Declared FHIR type applied to output | T106, T111 |
| FR-029 Untyped derivation fails with an actionable message, and an absent element is not such a column | T107, T112 |
| FR-030 An absent primitive carries its definition-derived type | T107, T108a, T130 |
| FR-031 Coding decoded by name | T083, T084, T091 |
| FR-032 Terminology on narrowed Coding | T085, T092, T093 |
| FR-033 Reference keys without a stored column | T088, T098, T094b |
| FR-034 Primitive id and extension navigation | T120–T124 (M5) |
| FR-035 Query-time decimal precision unchanged | T086, T094b, T095, T130, T086a, T095a |
| FR-036 Unnesting never reduced to a single leaf | T026, T027, T116 |
| FR-054 Traversal tolerant of an absent field, decided after schema resolution | T038a, T038b, T038d, T038e, T110, T110a (M2, all in Phase 7), T108 (M4) |
| FR-055 Absent elements typed by the definitions or the bottom type | T038c, T038e, T105, T110 |
| FR-056 Reconciliation by by-name projection, never by cast | T038j, T038k, T038l, T104a, T113a, T113b |
| FR-057 Canonical field order, covering the layout's own fields, wherever a structure type is produced | T030a, T038h, T038i, T104a, T118a |
| FR-058 One recursive field-wise merge, over a navigable canonical structure, serving reconciliation and file merging | T038f, T038g, T038h, T038i, T118a |
| FR-037 Unsupported layouts rejected with an actionable message | T042, T045, T046, T047 (read path only; the write path is FR-037a) |
| FR-037a Unsupported layouts rejected on the write path | T039a, T042a, T046a |
| FR-038 Detection structural, no extra file access | T044 |
| FR-039 Sparse and unclassifiable schemas accepted | T041 |
| FR-040 Detection disableable per source | T043, T048, T049 |
| FR-041 Divergent files read as one dataset | T114, T118, T118b, T118c, T119 |
| FR-042 Divergent append widens the table | T115, T117, T117a |
| FR-043 Encoding and decoding APIs preserved | T070, T081 |
| FR-044 Bounds options apply to dense only | T033 |
| FR-045 XML and Bundle ingest preserved | T058, T058a, T068, T069, T069a |
| FR-046 Earlier layouts not read as the new one | T049a (settled in Phase 7), T042, T045 |
| FR-047 Published layout contract replaced | T129 |
| FR-048 Dependency ban on the schema module | T005, T125 |
| FR-049 Import check on the new encoding | T007, T126 |
| FR-050 No encoder or hand-authored expression trees | T127 |
| FR-051 Existing implementation untouched | T128, T128a |
| FR-052 Query toolkit preserved and not relocated | T038e, T038l, T128, T128a |
| FR-053 Engine reads only the new layout | T100f, T100e (M6), T038m, T094b. T100 confirms the new layout is read on every path but **does not** assert the previous reader is gone; the engine reads both from M2 until T100e |

And the success criteria:

| Criterion | Tasks | Quickstart |
| --- | --- | --- |
| SC-001 Round trip over a real corpus, including the five decimal forms | T072, T076, T077 | QS-001 |
| SC-002 Suites pass over the new layout in pruned mode, dense subset green | T100, T037, T037a, T038, T100g | QS-003 |
| SC-003 Exclusion baselines gain no entries, obsolete ones removed | T100 | QS-003 |
| SC-004 Suite passes with every annotation disabled | T089 | QS-004 |
| SC-005 Divergent files return every resource with correct cardinality | T114, T115, T116 | QS-006 |
| SC-006 Earlier layouts rejected at read time with an actionable message | T042, T045–T047 | QS-008 |
| Follow-ups with a deadline and no other owner | T134b (the server's move), T134c (the unmeasured object-storage half of the T119 gate) | — |
| SC-007 Encode, decode, planning and execution measured separately | T002, T003, T136 | QS-010 |
| SC-008 No incompatible public API change in Java, Python or R | T049, T070, T081, T082, T100b | QS-002, QS-008 |
| SC-009 Build fails on internal Catalyst API in the schema module | T005, T125, T126 | QS-009 |
| SC-010 One column expression gives equal results on a fitted and a dense dataset | T038a, T038b (M2), T108 (M4) | QS-005 |

## Dependencies & Execution Order

### Across milestones

- **M1** depends on nothing. T001 must run before anything else — the baseline is
  unrecoverable once modules move.
- **M2** depends on M1 for the derived schema, the canonical structure and the
  shared merge. It does **not** depend on M3: nothing in the engine rewrite needs
  bundle or XML ingest.
- **M3** depends on M1: bundle and XML ingest both hand JSON to the M1 transform.
- **M4** depends on M1, M2 and M3. It must not open until the engine reads both
  layouts, a fitted schema behaves, divergent files read as one dataset, and no
  ingest format is stranded on the old path.
- **M5** depends on M4. Both gaps it closes are gaps in what M4 shipped.
- **M6** depends on M5 for T078c's closure of FR-017, and on T049a for T100e's
  scope.

### Within M1

- Phase 2 (definitions): T010–T012 (motion) before T013–T019 (widening).
- Phase 3 (derivation): depends on T016–T018. T038f and T038g are pure structure
  mechanics over `spark-sql-api` types and may run alongside; T038h and T038i need
  the definition widening, because the canonical structure is read from the
  definitions. T119 depends on T031 and nothing else.
- Phase 4 (US1): depends on Phase 3. T057b and T067a depend on T067.
- Phase 5 (US2): depends on Phase 4 — there is nothing to round-trip until data
  is written. T076 excludes `Bundle` resources, so it does not wait for M3.
- Phase 6 (US8): verification; depends on Phase 4 for something to inspect.

### Within M2

- T020–T027 (coverage) come first: they pin behaviour Phase 8 changes, and T024
  and T036 both touch the fixture mechanism, so the coverage must exist before it
  moves.
- T027a depends on T038e, so the coverage block is revisited once tolerant
  traversal exists; T027 itself does not wait.
- T049a is settled first. It sets the coverage standard for the Phase 8 dispatch
  arms (decision 51), so it cannot wait for Phase 13 where it is wired in.
- T101–T107 and T110–T113 (absent elements) come before the test infrastructure,
  not after it. Tolerance of an absent field is a precondition for moving any
  fixture, and it is owed to the **current** layout too: the existing encoder
  omits fields past its nesting bound, which is what `FhirViewExtraTest`'s two
  excluded cases under issue #2625 record. T110a decides which of the two
  missing-field mechanisms owns which site before T110 lands.
- T034–T038 and T037a (test infrastructure) depend on T031, from M1, and come
  last in the phase. Two independent dimensions, not one: T037's schema mode
  says how much of the layout is present, and T037a's layout says which
  conventions its fields follow. T037 may default to pruned because T110 has
  already landed. T037a must default to the **previous** layout, because the
  conventions are not dispatched until Phase 8, and T100g is what flips it.
- T038l depends on T038g and T038i, from M1, and takes the canonical structure
  through the interface declared in `utilities`. T038a–T038e and T038j–T038k
  depend on neither.
- Phase 8 (US3) depends on the whole of Phase 7. T083–T085 and T091–T093 (Coding by
  name) are correct under both layouts and may land at any point, including
  during M1; the remainder of the phase is the layout normalisation and cannot.
  T094a depends on T094 and on nothing else.
- T094b is the one place the two layouts meet (decision 55), so T095 to T098 are
  written against the new layout only and do not wait on each other. It depends
  on T038m having settled the lambda case, and on T094 if that gate says the
  extension branch needs the resource handle supplied rather than derived.
- Phase 9 (US4) depends on T038l for the reconciliation expression, not on the
  absent-element work that moved out of it.
- Phase 10 (US5): T114, T115, T117, T117a, T118 and T118a depend on M1 for a
  write path — `io`'s own entry points, since the public API does not switch
  until M4 — and T116 depends on the absent-element work now in Phase 7. T119 measured the
  merge cost in M1, so its answer is in hand: it is what fixes the shape of
  T118's opt-out and what puts T118b on the list.

### Within M4

- T049a was settled in Phase 7. Phase 13 asserts whatever it decided.
- T042a and T046a extend detection to the write path, which FR-037 and SC-006
  do not reach. They must land with T117 and T117a, or before them, since it is
  auto-merge that makes an undetected write into an earlier-layout table widen
  it rather than fail.
- Phase 13 (US7) depends only on Setup and may be built at any point, but must
  not be wired in before Phase 12.
- Phase 12 (the switch) depends on Phases 8, 9, 10, 11 and 13. It is the point of
  no return: after it, the public API writes the new layout.
- T108 and T108a depend on T070 and T081, because they assert the guarantee
  through the public API.
- Phase 14 depends on Phase 12; T136 measures what Phase 12 shipped.

### Within M5 and M6

- Phase 15: each annotation kind is independent of the others **after the
  first**. T063's registry and T055's toggle test are shared, so they ride with
  whichever kind ships first. The emission task
  precedes its fast-path task in every case. T090 depends on any one kind having
  landed, since it needs an annotated input to compare against.
- Phase 16 depends on T094, from Phase 8, and T078c depends on T051 and T061.
- T100f gates T100e. T100e depends on T049a's answer for its scope.

### The risk gate

**T038a gates the approach.** It is the only untested risk in the chosen design:
if the tolerant traversal expression cannot survive the analyzer over an
unresolved child, the fallback is option C in R-017 — the unbound column
representation — and T038e, T038l, T110, T113a and T113b are rewritten against
it. The representation of absence (FR-055) and the reconciliation mechanism
(FR-056 to FR-058) are unaffected either way, which is why this gate is cheap to
fail.

The milestone structure shrinks what is at stake: nothing in M1 depends on the
answer. **T009a resolved it in Phase 1 nonetheless**, as a throwaway spike, and
it passed: the expression survives the analyzer and the design stays on option D.
T038a remains the durable test in Phase 7.

### Why there is no red window

**Every milestone ends green, and no milestone runs red in the middle.** Three
hazards are designed out rather than accepted. Each is a separate axis, and an
argument that covers one does not cover another.

**Absence.** A schema fitted to a small fixture set omits most of each resource,
so an expression naming an omitted element would meet a Spark analysis failure
rather than an empty collection. T110 emits the tolerant traversal expression at
every step and is what makes it empty instead. It sits in Phase 7, ahead of the
test infrastructure, so no fixture ever moves onto a schema the engine cannot
traverse. This axis is not peculiar to the fitted schema: the existing encoder
omits fields past its nesting bound too, so T110 also closes the two
`FhirViewExtraTest` cases excluded under issue #2625.

**Conventions.** A field present in a *different shape* is a different problem
from a field that is absent, and tolerant traversal does nothing for it. A
lexical decimal is a string where the engine expects a fixed-precision numeric,
an extension has no `_fid` to key the root map on, and a quantity has no
canonicalised companion. Those are the Phase 8 dispatch arms, T095 to T098. The
T037a layout dimension is what keeps the estate off those conventions until the
arms exist: were the fixtures moved first, a decimal comparison would not fail
loudly, it would silently compare strings.

**The previous layout.** Phase 8 converts by addition rather than replacement.
Every traversal dispatches on the resolved schema, so after T095 the engine still
reads a previous-layout decimal and after T097 it still reaches an extension
through a field identifier. Tests that encode through `PathlingContext` and then
query keep working for the whole of M2, because `PathlingContext` keeps writing
the previous layout and the engine keeps reading it.

This reverses decision 40, which accepted a red span running from the engine
foundations to the switch under the previous sequencing. The cost is recorded
instead in decision 51: the dispatch arms exist to buy this, and T100e removes
them in M6.

Within each story: tests written and failing, then implementation. Tasks touching
the same file run sequentially; `[P]` tasks on different files may run together.

## Implementation Strategy

**M1 is the defensible increment.** Data conforming to Parquet on FHIR is written
and read back over a real corpus, and the module boundary is enforced by the
build — with the public API, the engine and every existing user path untouched.
It stands alone even if nothing else lands, and it cannot break an
encode-then-query path, because it does not rewire the encoder. Its round trip is
lossless but for primitive ids and extensions, which FR-017's carve-out records
and T057b keeps visible.

**M2 rewrites the engine without moving the layout.** The engine learns to read
the new layout while keeping the previous one, which is what lets the whole
rewrite happen behind a green build and gives a genuine before-and-after over the
same data for T110 — the riskiest change in the programme. Nothing here is
user-visible.

**M3 closes the ingest surface** so the flag day does not strand bundles or XML
on the old path. Five tasks, exercised afterwards through the M1 round-trip
harness.

**M4 is the flag day, and the only breaking release.** The public API switches,
detection of earlier layouts is armed beside it, and the layout is documented in
the same step. Everything it depends on is already in place, so the switch is a
writer flip rather than a migration. It ships annotation-free and without
primitive metadata: FR-022 makes the first safe, and FR-017's carve-out bounds
the second.

**M5 closes those two gaps, one increment at a time.** T136 at the flip says
which annotation to build first. Each kind ships on its own, and primitive ids
and extensions follow.

**M6 retires the debt.** The previous-layout reader and its dispatch arms come
out, FR-053 reaches its end state, and the superseded design is retired.

The server pins to the last library release before Phase 12 — the first change
users can observe — and must reach the post-change line before the pinned line
stops receiving fixes.
