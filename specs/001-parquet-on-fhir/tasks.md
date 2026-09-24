# Tasks: Parquet on FHIR

**Input**: Design documents in `specs/001-parquet-on-fhir/`
**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/](contracts/)

**Tests**: Test-driven development is mandatory. Every behaviour is covered by a
test task that precedes its implementation task, and those tests must be written
to fail first.

Three categories are exempt and say so where they appear:

- **Pure motion** (T010–T012): the requirement is that behaviour does not
  change, so _the existing suite is the test_. Verification is that no test
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

Seven. **M1, M3, M6 and M7 change nothing a user can observe, and each is
independently shippable. M2 changes two things, both in Phase 10 and both named
in its row. M4 is the only breaking release.** M5 lands as a series
of independent increments: each annotation kind ships on its own, and primitive
ids and extensions follow them. M6 adds a mode beside the existing one rather
than changing it, which is why it joins the invisible set.

**Task IDs are never renumbered and never reused.** They are referenced
throughout this document and by both traceability tables, so an addition takes a
letter suffix and sits beside the task it belongs with. Identifiers therefore do
not run in numeric order, and the phase map below rather than the numbering is
what says which tasks are where.

| Milestone                                                                                 | Phases | Tasks | Delivers                                                                                                                                                                                                         | User-visible change                                                                                                                                                                                                                                            |
| ----------------------------------------------------------------------------------------- | ------ | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **M1 The layout** [#2761](https://github.com/aehrc/pathling/issues/2761)                  | 1–6    | 72    | FHIR JSON to the new layout and back through `io`, losslessly except the exceptions FR-016 lists. No annotations, and the pruned schema mode only. One definition traversal, the canonical structure and the one shared merge. Both risk gates resolved | None                                                                                                                                                                                                                                                           |
| **M2 The engine, layout-tolerant** [#2762](https://github.com/aehrc/pathling/issues/2762) | 7–10   | 70    | The engine reads both layouts by dispatch, computes every value without annotations, tolerates a fitted schema, and reads divergent files as one dataset. Green on the previous layout throughout                | Two, both in Phase 10: the Delta upsert path widens the target where it used to refuse (T117a, decision 41), and reading raw files merges their schemas with a supplied-schema opt-out (T118). Move those three tasks to M4 if the milestone must be invisible |
| **M3 Ingest formats** [#2763](https://github.com/aehrc/pathling/issues/2763)              | 11     | 5     | Bundles and XML on the new path, each round-tripped through the M1 harness                                                                                                                                       | None                                                                                                                                                                                                                                                           |
| **M4 The flip** [#2764](https://github.com/aehrc/pathling/issues/2764)                    | 12–14  | 28    | The public API writes the new layout, earlier layouts are detected, the layout is documented and the benchmark is recorded                                                                                       | The flag day                                                                                                                                                                                                                                                   |
| **M5 The gaps** [#2765](https://github.com/aehrc/pathling/issues/2765)                    | 15–16  | 21    | Annotations emitted and read, one kind at a time; primitive ids and extensions written and navigable                                                                                                             | Performance, then new capability                                                                                                                                                                                                                               |
| **M6 The dense schema** [#2771](https://github.com/aehrc/pathling/issues/2771)            | 17     | 8     | The second schema mode: a bounded cursor over the definitions, the strategy interface extracted with both implementations in hand, and the three bounds options given something to bound again (FR-009, FR-010, FR-044)   | None. The three bounds options bound something again and a second mode becomes selectable; nothing changes for a caller who leaves them at their defaults                                                                                                      |
| **M7 Completion** [#2766](https://github.com/aehrc/pathling/issues/2766)                  | 18     | 17    | The previous-layout reader removed, FR-053 tightened, the superseded design retired, follow-ups raised                                                                                                           | None, **contingent on T049a**: if the source boundary routes earlier-layout data rather than refusing it, the reader T100e removes is a product feature and its removal is breaking                                                                            |

**Each milestone is a sub-issue of #2367 and a pull request against
`issue/2367`**, which holds the specification and no implementation. The
numbers are in the table above. Decision 65 records why the delivery is
structured this way, and what it changes about the review.

### Phase map

| Phase                                                                | Tasks                                                                                            | Count |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ----- |
| 1 Setup                                                              | T001–T009, T009a                                                                                 | 10    |
| 2 The definition abstraction                                         | T010–T019                                                                                        | 10    |
| 3 Schema derivation                                                  | T028, T029, T030a, T031, T031b, T031c, T032, T033b, T033c, T038f–T038i, T119                     | 14    |
| 4 US1 The storage layout                                             | T050, T052, T056, T057, T057a–T057e, T059, T059a, T060, T060a, T060b, T062, T067, T067a          | 17    |
| 5 US2 Lossless round trip                                            | T071–T078, T078a, T078b, T078d, T079, T080, T080a–T080d                                          | 17    |
| 6 US8 The module boundary                                            | T125–T128                                                                                        | 4     |
| 7 Engine foundations                                                 | T020–T027, T027a, T049a, T034–T038, T037a, T038a–T038e, T038j–T038m, T110a, T101–T107, T110–T113 | 37    |
| 8 US3 The engine reads both layouts                                  | T083–T089, T089a, T091–T094, T094a, T094b, T095–T099                                             | 19    |
| 9 US4 Shape reconciliation (US4's absent-element half is in Phase 7) | T104a, T104b, T109, T113a, T113b                                                                 | 5     |
| 10 US5 Divergent files read as one dataset                           | T114–T117, T117a, T118, T118a–T118c                                                              | 9     |
| 11 Remaining ingest formats                                          | T058, T058a, T068, T069, T069a                                                                   | 5     |
| 12 The public API switch                                             | T070, T081, T082, T100, T100b, T100c, T100d, T100g, T108, T108a, T128a                           | 11    |
| 13 US7 Detection of earlier layouts                                  | T039, T039a, T040–T042, T042a, T043–T046, T046a, T047–T049                                       | 14    |
| 14 Documentation and measurement                                     | T129, T130, T136                                                                                 | 3     |
| 15 Annotations                                                       | T050a, T053–T055, T063–T066, T086a, T087a, T090, T095a, T096a                                    | 13    |
| 16 US6 Primitive ids and extensions                                  | T051, T061, T078c, T120–T124                                                                     | 8     |
| 17 The dense schema                                                  | T030, T031a, T031d, T031e, T033, T033a, T037b, T108b                                             | 8     |
| 18 Completion                                                        | T100f, T100e, T131–T134, T134a–T134h, T135, T137–T139                                            | 18    |

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
**There is therefore no window without a green build**, which reverses decision 40. Decision 52 records the three axes this rests on.

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
engine, because the ingest transform writes _into_ the derived schema and
FR-008/FR-012 forbid taking types or cardinality from the data. The structure
merge and the canonical structure (T038f–T038i) are in M1 for the same reason. The
merge and its interface are pure structure mechanics over `spark-sql-api` types
and live in `utilities`, so that `encoders` reaches the interface without
depending on `fhir-schema`. Decision 68 makes the canonical structure the whole
of the derivation: the transform's traversal *is* a walk of it, so there is one
notion of canonical order rather than two that have to be kept in step.

**The dense schema mode belongs to M6** (decision 69). It leaves M1 because with
`SchemaBuilder` deleted there is no second derivation for its absence to leave
behind: one traversal with one strategy is what M1 ships, and the second strategy
is additive. It lands after M5 rather than before it because M5 is where the
layout's width is settled — annotations and the primitive metadata group each add
columns — so the bounded cursor is built once, against the final column set. The
price is that FR-009 and FR-044 go unmet from the flip until M6, and the three
bounds options stay in the public API bounding nothing; decision 69 records why
they are kept rather than deprecated, and what M5 must decide so that M6 is not
retrofitting.

---

# Milestone 1 — The layout

Data conforming to Parquet on FHIR is written and read back losslessly.
Primitive ids and extensions and every annotation are deliberately left to M5,
and the dense schema mode to M6. Nothing a user can see changes.

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
- [x] T009 Add `fhir-schema` as a dependency of `fhirpath` in `fhirpath/pom.xml`. `io` is added only when the engine's tests first need data in the new layout (decision 67). `encoders/pom.xml` is not modified.
- [x] T009a **Spike, and the gate on the whole approach.** Build a throwaway `RuntimeReplaceable` over an _unresolved_ attribute and confirm it survives the analyzer — that nothing probes `dataType` before the child resolves. Discard the code; T038a is the durable test. Run it here rather than in Phase 7 because it is the programme's largest unknown and costs a day, so the answer is worth having while M1 still has planning time. **On failure**: the design falls back to the unbound column representation in R-017 option C, and T038e, T038l, T110, T113a and T113b are rewritten against it before M2 opens. FR-055 to FR-058 are unaffected either way.

**Checkpoint**: Modules exist and are empty; the boundary is enforced; the baseline is captured; the analyzer risk is resolved.

---

## Phase 2: The definition abstraction

**Purpose**: Move the definitions below the engine and widen them enough to drive
schema derivation. Pure motion first, then widening.

- [x] T010 Move `fhirpath/src/main/java/au/csiro/pathling/fhirpath/definition/**` (including `defaults/` and `fhir/`) to `fhir-schema/src/main/java/au/csiro/pathling/definition/`. Pure motion — no test task; behaviour is unchanged by construction.
- [x] T011 Update the referencing files in `fhirpath` main and test to the new package.
- [x] T012 Verify the motion by reviewing the test diff: it must contain only package declarations and import statements. This is the test for T010–T011. **One exception, found by M1's review of the rescoped implementation**: `fhir-schema/src/test/java/au/csiro/pathling/definition/fhir/NodeDefinitionTest.java` is rewritten rather than moved, in commit `e9e3eef174`. It drops `@SpringBootUnitTest`, which `fhir-schema` cannot carry because it does not depend on Spring, and uses the plain session the module's other tests use instead. It obtains its definitions through `DefinitionContext.findResourceDefinition` rather than constructing `FhirResourceDefinition` directly, and the constructor it used is deleted from `FhirResourceDefinition`, which nothing else called. Its three test methods are renamed to say what they assert. Every assertion in it is preserved.
- [x] T013 Test child enumeration in `fhir-schema/src/test/java/au/csiro/pathling/definition/NodeDefinitionTest.java` — children of a resource, a backbone element and a complex type, asserting order and completeness against the FHIR definitions, and a choice element appearing once as a choice rather than pre-expanded.
- [x] T014 Test cardinality in `fhir-schema/src/test/java/au/csiro/pathling/definition/ElementDefinitionTest.java` — singular, repeating, and choice elements.
- [x] T015 Test that both implementations agree on enumeration and cardinality for the same resource, in `fhir-schema/src/test/java/au/csiro/pathling/definition/DefinitionContextAgreementTest.java`, and that every element the R4 definitions describe reports its type, in `fhir-schema/src/test/java/au/csiro/pathling/definition/FhirTypeCoverageTest.java`.
- [x] T016 Add child enumeration to `fhir-schema/src/main/java/au/csiro/pathling/definition/NodeDefinition.java`, implemented from the children the HAPI-backed implementation already obtains rather than asking a second time.
- [x] T017 Add cardinality to `fhir-schema/src/main/java/au/csiro/pathling/definition/ElementDefinition.java`.
- [x] T018 *Withdrawn by decision 67.* The reported type stays the R4 enumeration; no module-local representation is introduced.
- [x] T019 *Withdrawn by decision 67.* The engine's call sites are unchanged beyond T011's imports. The engine continues to read cardinality from the Spark schema in this phase.

**Checkpoint**: Definitions live below the engine and report children and cardinality. The engine differs from `main` only in its imports.

---

## Phase 3: Schema derivation

**Purpose**: Turn definitions into a Spark schema, in the pruned mode, with the
navigable canonical structure and the structure merge that every later consumer
shares. The dense mode is the second strategy over the same traversal and is
delivered in M6 (decision 69).

_Moved to Phase 17 by decision 69: T030 (mode parity), T031a (the bounded
cursor), T033 and T033a (the configuration). T030a and T031c stay, reduced to
what a single mode can assert._

- [x] T028 **Rewritten by decision 68**: the same properties, asserted against `transform(...).schema` rather than against `SchemaBuilder`. Types and cardinality taken from definitions, never from data; a repeating element is an array whether one value or many are present. The schema is no longer derived as a value, so what is tested is the schema that is actually written. Moves out of `SchemaBuilderTest` and into the `io` transform tests, which is where a session and a fixture are already available.
- [x] T029 **Rewritten by decision 68**: FR-011's rule — a complex element survives only where some descendant leaf is populated, so a field-less structure never arises — becomes the post-order result of the single traversal rather than a filter pass, and is asserted against the transform's output schema.
- [x] T030a **Rewritten by decision 68, reduced by decision 69**: every structure the transform produces orders its fields in definition order (FR-057). Field order is part of the type, so this is a correctness assertion rather than a tidiness one. The subsequence half — that a pruned structure's order is a subsequence of the dense one — needs a dense structure to be a subsequence of, and is asserted by T030 in M6.
- [x] **T031 — withdrawn by decision 68.** `SchemaBuilder` is deleted; there is no derivation step to implement. Superseded by T031a and T031b.
- [x] **T032 — withdrawn by decision 68.** `SchemaPruner` is deleted; pruning is what the traversal does when its strategy is the observed schema.
- [x] T031b Implement the single definition-driven traversal in `io`, returning `Optional<Column>` per node, replacing derivation followed by projection. The observed schema is an argument threaded through the walk rather than a modelled strategy: where it lacks a child, the node does not appear. The stored schema is `select(columns).schema` and is never constructed. **Amended by decision 69**: the dense arm is T031e in M6, so nothing here needs a type for a node the data does not carry and there is no `StructType` to materialise for one. Walk `DefinitionCanonicalStructure` directly rather than declaring a strategy interface from a single implementation — T031a extracts it with two in hand. The traversal needs `LayoutEntry`, which carries the FHIR type, the cardinality and the entry kind, so it is **not** written against `utilities`' `CanonicalStructure`; that interface keeps serving the merge untouched (decision 69, correcting decision 68).
- [x] T031c [P] Test that the unbounded `CanonicalStructure` terminates when descending a self-recursive type, under T038h's existing lazy-expansion assertion. A cursor bug on a cyclic graph does not fail, it hangs, so this cannot be left to the suite's other tests to catch (decision 68). The bounded cursor's termination test is T031d, and lands with the cursor in M6.
- [x] T033c **New, from decision 69.** Delete `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaConfiguration.java` and `SchemaConfigurationTest`. The class has no live field left: the strictness switch went with decision 68, the annotation toggles belong to M5 and the schema mode to M6. It is deleted rather than reduced to a one-position switch, and `ResourceTransformer.of` takes the definitions alone. M5 recreates it for the annotation toggles at T063, and T033 adds the schema mode to it in M6.
- [x] T033b Classify every test class **outside `fhirpath`** that reaches the existing encoder, recording the result in `evidence/encoder-scope.md` under the category scheme already there. Split them into those that recover unaided once the public API switches and those that assert behaviour specific to the previous layout and must be converted or retired. Named starting points: `NarrowMergeTest`, `MigratedTableDecodingTest` and `ExtensionContexts` in `library-api`, the `SchemaMisalignment` helper they reach in the `encoders` test jar, and the test-jar dependencies declared by `lib/R/pom.xml`. T100d acts on the second group. Doing this in M1 sizes the flag day while there is still planning time.
- [x] T038f [P] Test the structure merge in `utilities/src/test/java/au/csiro/pathling/utilities/StructureMergeTest.java` — recursive field-wise union under a supplied canonical structure, and therefore commutative and associative regardless of merge order (FR-058). Cover **nested depth**, not one level: a merge that orders the top level correctly and the levels beneath it by discovery order passes a single-level test and is wrong. Drive it with a hand-built canonical structure so the test does not depend on `fhir-schema`.
- [x] T038g Declare the canonical structure interface in `utilities/src/main/java/au/csiro/pathling/utilities/CanonicalStructure.java` and implement the merge in `utilities/src/main/java/au/csiro/pathling/utilities/StructureMerge.java`, as the single implementation serving both reconciliation and the merging of divergent file schemas (FR-058). The interface answers two questions at any node: the canonical field order here, and the structure under a given field name. It must be **lazily navigable**, because FHIR's definition graph is cyclic — extensions are self-recursive, and a reference carries an identifier that carries a reference — so the expanded tree is infinite and the depth the merge needs is set by its operands, not known statically. Both live in `utilities`, not `fhir-schema`, because both callers must reach them and `encoders` must see only the interface, never `fhir-schema` (T009, FR-051). Pure structure mechanics over `spark-sql-api` types.
- [x] T038h [P] Test the definition-backed canonical structure in `fhir-schema/src/test/java/au/csiro/pathling/schema/DefinitionCanonicalStructureTest.java` — the field order at a node is definition order with the layout's own fields at their fixed positions, the annotations and the metadata group beside a primitive (FR-057); descent by field name reaches the structure for that element; and descending a self-recursive type to an arbitrary depth terminates without forcing an expansion beyond the depth asked for. Assert that merging two pruned structures under it yields definition order rather than discovery order. This is the input the merge cannot derive: two subsequences of a total order do not determine it, since `[id, family]` and `[id, given]` do not say which of `family` and `given` comes first.
- [x] T038i Implement the interface over the definitions in `fhir-schema/src/main/java/au/csiro/pathling/schema/DefinitionCanonicalStructure.java` (FR-057). **Satisfied vacuously under decision 68**: "route `SchemaBuilder` through the same traversal" is met by there being no `SchemaBuilder` — the transform walks this interface directly. Its memoisation note does **not** transfer to the bounded implementation: decision 69 corrects decision 68 here, because the nesting bound counts recurrences per type identity along the path rather than a scalar depth, so no key captures it and T031a's cursor is not memoised at all. It sits beside derivation because derivation already walks the same cyclic graph and must decide the same positions; sharing the traversal is what stops the two notions of canonical order drifting apart, and structures compare positionally when they do. Memoise per FHIR type, since a merge descends the same types repeatedly.
- [x] T119 Measure merge cost over a realistic file count on object storage and record it in `evidence/merge-cost.md`, using synthetic files carrying a derived schema. The result can force a fallback for raw files (R-015). Measured here, not in US5, because it is a gate: merge cost is a property of file count and schema width, so it needs T031 and nothing else, and T118 should be designed around the answer rather than rewritten after it.

**Checkpoint**: Definitions drive schema derivation in both modes; one navigable canonical structure serves derivation, reconciliation and file merging; one merge serves every later consumer; merge cost is known.

---

## Phase 4: User Story 1 - The storage layout (Priority: P1)

**Goal**: FHIR JSON is stored in the Parquet on FHIR layout.

**Independent Test**: Load a corpus through `io` and inspect the stored schema and
values against the contract, with no FHIRPath evaluation.

_Bundle and XML ingest are deferred to M3 (T058, T068, T069), and the public API
rewiring (T070) to M4. Every annotation is deferred to M5 (T050a, T053–T055,
T063–T066), as is the primitive metadata group (T051, T061); FR-022 makes the
first safe and FR-017's carve-out bounds the second, which is what T057b and
T067a enforce._

### Tests ⚠️ write first, confirm failing

- [x] T050 [P] [US1] **Reworded by decision 68**: test that a decimal is stored as text numerically equal to its source, in `io/src/test/java/au/csiro/pathling/io/transform/PrimitiveConverterTest.java` beside T057d's decimal cases. `DecimalTransformTest` went with the class it tested; T050a recreates a file of that name in M5 for the annotation. The source's lexical form is not preserved; the stored text is what a double round trip produces, measured in `evidence/scripts/double_serde.py`. The numeric annotation beside it is T050a, in M5.
- [x] T052 [P] [US1] Test that extensions on complex elements are stored inline, and that no field identifier and no root-level extension map are emitted, in `io/src/test/java/au/csiro/pathling/io/transform/InlineExtensionStorageTest.java`. These tests are where inline storage is stated, because T062 showed there is no extension component to point at.
- [x] T056 [P] [US1] **Reworded by decision 68**: test that `contained` resources are detected and reported as a warning, never silently dropped, in `io/src/test/java/au/csiro/pathling/io/transform/StrictnessTest.java`. The switch is gone; the detection is not.
- [x] T057 [P] [US1] **Reworded by decision 68**: test that content the definition set does not describe is ignored and reported as a warning, in the same file. There is no failing mode to assert. Detection must compare observed keys against the definitions: the JSON reader silently skips unknown fields in every mode, so it cannot be asked to enforce this.
- [x] T057a [P] [US1] **Reworded by decision 68**: test that input whose cardinality contradicts the definitions — a repeating element supplied as a single object rather than a one-element array — is nulled and reported rather than silently coerced, in the same file. Conformant FHIR JSON always uses an array for a repeating element, so inference gets cardinality right for conformant input; this is the case where it does not, and it surfaces in the transform rather than at the read.
- [x] T057b [P] [US1] **Reworded by decision 68**: test that primitive id and extension content present in the input is reported as a warning rather than silently dropped, in the same file. The traversal declines to emit the metadata group until M5, so this content is dropped; FR-017's carve-out requires the loss be reported. T078c removes this task's reason to exist.
- [x] T057d [P] [US1] **New, from decision 68.** Test the converter table in `io/src/test/java/au/csiro/pathling/io/transform/PrimitiveConverterTest.java`: one case per FHIR primitive type, in **both cardinalities**, asserting the stored SQL type and the value. Twenty-one types, each a named entry rather than a default branch. Cover the acceptance rule as well as the happy path — `decimal` accepting `DoubleType`, `LongType` and `DecimalType`, an `integer` column re-typed to `DoubleType` by one fractional value being nulled for the whole column, `try_cast` nulling an integer beyond 2³¹, and a `StringType` column against a `boolean` element being nulled rather than casting `"yes"` to true. A totality test beside `FhirTypeCoverageTest` asserts the table covers every primitive the definitions can report, so a type added to `PrimitiveTypes` without a converter fails the build.
- [x] T057e [P] [US1] **New, from the PR review of `789d13b347`.** Close two ways content was ignored without a finding, in `io/src/test/java/au/csiro/pathling/io/transform/StrictnessTest.java`. First, an array of arrays where the definitions declare a repeating element — `"given":[["a","b"]]`, `"name":[[{...}]]`, `"careTeamSequence":[[1,2]]` — passed the cardinality check, which compared only the outer level, and the value was then unwrapped through every level: a text value was stored as the text of its inner array, a structure's fields were read one level too deep, and a number failed the whole read with a Spark analysis error. FHIR JSON has no arrays of arrays, so `StrictnessCheck.isConformingCardinality` now requires exactly one array level for a repeating element, and both the check and `ResourceTransformer.shapeMatches` use it: the nesting is reported as a shape mismatch and stored as a typed null, as T057a's single value is. Second, a source key named like one of the layout's own annotations — `__birthDate_start`, `__valueQuantity_canonical` — was found in the canonical structure, checked as the element it annotates, and then dropped by the transform, which stores only elements. An annotation is derived, never read, so such a key is now reported as undescribed content (T057). JSON is the only source `toLayout` reads in M1, so no producer supplies these keys legitimately and the finding cannot be a false positive.
- [x] **T057c — withdrawn by decision 68.** Value validation has no warning-only form and is deferred: a value contradicting its declared type is now cast and nulled rather than named. `PrimitiveValues`' `LEXICAL_FORMS` and `raise_error` path are deleted with it, and decision 57's addendum records what is lost — value-range checks, and per-value rather than per-column granularity. T134g carries the follow-up. Superseded by T057d.

### Implementation

- [x] T059 [US1] **Amended by decision 68, relocated by decision 70** — the read is now `FhirJsonReader` and the transform `ResourceTransformer.toLayout`: implement the JSON read and the transform in `io/src/main/java/au/csiro/pathling/io/transform/ResourceTransformer.java`. Read with an inferred schema — **without** `primitivesAsString`, so a decimal arrives as `DoubleType` or `LongType` and every other primitive as the type its JSON encoding implies — then impose types, cardinality, conventions and field order from the definitions through the single traversal of T031b (R-008). The observed schema decides presence and nothing else (FR-008, FR-012). `stored()` and its `IllegalStateException` are deleted: with one walk there are no longer two notions of presence to keep in step. This is the entry point M1, M2 and M3 are exercised through, since the public API is not rewired until M4.
- [x] **T059a — withdrawn by decision 68.** The lexical-form check is deleted. What replaces it is the converter table's per-column class check in T057d and T060b, which subsumes the cases the regexes were written for without validating values.
- [x] T060 [P] [US1] **Reworded by decision 68**: implement the decimal transform as a cast in each direction — inferred `DoubleType`, `LongType` or `DecimalType` to `StringType` on ingest (the last by the addendum to decision 68), and stored text back to `DoubleType` at egress, so the JSON writer emits a bare number. `DecimalTransform`'s marking, unmarking and `primitivesAsString` option are deleted, and with them a `regexp_replace` pass over every serialised document.
- [x] T060a [P] [US1] **New, from decision 68.** Implement the `base64Binary` converter: `unbase64` on ingest, storing `BinaryType`, and `base64` wrapped in `regexp_replace(..., "[\r\n]", "")` at egress. `PrimitiveTypes` changes from `StringType` to `BinaryType`, which removes an unrecorded deviation from the Parquet on FHIR specification — it maps `base64Binary` to Parquet `binary` with **no** logical type, and Spark's `StringType` writes `BINARY` annotated `STRING` — and converges with the existing encoder, which already stores `BinaryType` (`R4DataTypeMappings.scala:217`). The newline strip is required: `spark.sql.chunkBase64String.enabled` defaults to true on Spark 4.0.2 (`SQLConf.scala:3831-3837`). Note the round-trip consequence in FR-016's exception list: FHIR permits whitespace inside a base64 value and re-encoding canonicalises it away. Ingest uses `try_to_binary(value, 'base64')` rather than `unbase64`, so that malformed content becomes null, as every other converter's `try_cast` does, rather than raising.
- [x] T060b [US1] **New, from decision 68.** Implement the converter table — a map from FHIR primitive type to a converter, with an entry named for each of the twenty-one primitives and no default branch, in the spike's idiom. Each converter states what inferred types it accepts and what it stores, and casts with `try_cast` so that a value it cannot represent becomes null rather than raising or truncating. The acceptance check is per column, by shape and class: `shapeMatches`'s existing cardinality and structure checks, plus the inferred type against what the element's encoding admits.
- [x] T062 [P] [US1] Implement the extension transform — **no implementation of its own, which is a result rather than a gap**. The definitions describe an extension as an ordinary structure and this layout stores it where they place it, so the generic structural mapping in T059 already is inline storage; an extension-specific path was written, measured to have no observable effect, and removed. T052 is what pins the behaviour. The previous layout needed such a path only because it hoisted extensions into a map at the root of the resource. Extensions on _primitive_ elements are the other half of FR-003 and land in `PrimitiveMetadataTransform` at T061, not here.
- [x] T067 [US1] Implement strictness checking against the definitions, including `contained` detection, in `io/src/main/java/au/csiro/pathling/io/transform/StrictnessCheck.java`. **Stands under decision 68**: all four branches survive, because a warning still has to name what it dropped, so this task needs no further work. The raising branch of `ResourceTransformer.report()` goes, but it lives in `ResourceTransformer` and is tracked under T059; the switch it reads goes under T033.
- [x] T067a [US1] Implement detection of primitive id and extension content in the input, reported as a warning, in the same file. **Stands under decision 68.**

**Checkpoint**: JSON is written in the new layout through `io`, without annotations and without primitive metadata. The public API is unchanged and the engine still reads the previous layout.

---

## Phase 5: User Story 2 - Lossless round trip (Priority: P1)

**Goal**: Conformant FHIR JSON round-trips to a semantically equal resource.

**Independent Test**: Round-trip a real corpus through `io`, asserting semantic
equality resource by resource, with no FHIRPath evaluation.

_The public API rewiring (T081, T082) is deferred to M4; the harness drives
`io`'s reader and writer directly, so it does not need it.
The harness also excludes primitive id and extension content, which is not
written until M5 — the exclusion is explicit and asserted, and T078c removes it._

### Tests ⚠️ write first, confirm failing

- [x] T071 [US2] **Amended by decision 68**: build the round-trip harness in `io/src/test/java/au/csiro/pathling/io/RoundTripHarness.java`, comparing semantically: object key order ignored, array order significant, numbers compared **numerically** as `Double.parseDouble(a) == Double.parseDouble(b)`, which needs no tolerance. `SemanticJson` keeps its kind comparison, because it is what stops a serialiser that quotes every number from passing a comparison built on text. The harness carries FR-016's exception list, each exclusion asserted rather than incidental.
- [x] T072 [P] [US2] **Amended by decision 68**: test decimals through the harness — a trailing zero, exponent notation, a leading sign, a very small magnitude and forty significant digits — under numeric comparison, in `io/src/test/java/au/csiro/pathling/io/DecimalRoundTripTest.java`. All five survive as numerically equal; none survives as textually identical. `evidence/scripts/double_serde.py` records what each stores and writes.
- [x] **T073 — withdrawn by decision 71.** The shapes this asserted are the ones the input is now assumed not to carry, and `EgressOmissionTest` is deleted with them. An absent element is still absent from the output, which the round-trip harness asserts on every corpus; what goes is the class that built the excluded shapes by hand.
- [x] **T074 — withdrawn by decision 71.** Such a structure is now written as an empty object, and the input is assumed not to produce one.
- [x] **T075 — withdrawn by decision 71.** Such an array is now written as it is stored, and the input is assumed not to produce one.
- [x] T076 [US2] **Reduced by decision 68**: run the harness over a structural subset of the corpus rather than all of it, and add `base64Binary` whitespace, the decimal forms and the re-typed-column case to the asserted exclusions. The corpus is kept rather than trimmed on disk, and the subset chosen within it, so that restoring full coverage is a change to the selection and not a re-vendoring. It is reduced and not removed because it is hand-authored specification material rather than synthetic, and it is what found decision 64 and the `ActivityDefinition.timingTiming` cascade, neither of which a unit test would have produced. Run the harness over the FHIR R4 specification examples in `io/src/test/java/au/csiro/pathling/io/SpecExampleRoundTripTest.java`. The corpus is a curated subset vendored at `io/src/test/resources/data/fhir-spec/R4/`: 161 resources, 541 KB, drawn from the 2,912 files of `examples-json.zip` by taking the smallest hand-authored example of every one of the 140 non-`Bundle` resource types and then a greedy cover of 79 named structural corners. `MANIFEST.tsv` records what each file is there to prove and `README.md` the provenance, the selection principle and the packaging — which removes newlines and nothing else, because anything that parsed a value would normalise `1.50` to `1.5` and re-render `1e2`. Four exclusions apply, each asserted rather than incidental: `Bundle` (FR-007, permanent); `contained` resources (FR-006, permanent, via the harness's `excludingContainedResources()`); primitive id and extension content (FR-017's carve-out, removed by T078c); and `Parameters` from the comparison alone, its detection still asserted, until decision 64 is settled by T134e. Two things the corpus found are recorded rather than absorbed: decision 64, and the cascade the metadata exclusion needs where an element's entire content was metadata — `ActivityDefinition.timingTiming` holds only `_event`, so the layout correctly omits it under FR-019 while the harness was comparing an empty object against a correct absence. *(Decision 72 reverses this: the layout keeps `timingTiming` as `{}`, as FHIR does, and the cascade is gone. The released encoder drops it, because the parser it reads through discards an `_event` array with no `event` beside it; T101 records that divergence.)* Decision 63's expectation that this corpus would expose the positional-null collision was measured and did not hold; the amendment there records why the hazard nonetheless stands for T061. The round trip runs over the fewest types that between them cover every corner `MANIFEST.tsv` names — a greedy cover computed from the manifest, 25 of the 139 round-trippable types — while detection still runs over every type; the class runs 168 tests in around 25 seconds, down from 281 in around 100. Restoring full coverage is returning every round-trippable type from the selection. The exceptions every round trip carries are asserted alongside the four exclusions: a type whose examples carry `decimal.trailing-zero` or `decimal.exponent` must return a number that matched only numerically, and no example carries whitespace inside a base64 value, which is recorded as an empty expectation rather than assumed. The re-typed-column and ignored-content exclusions have no occasion in this corpus and are asserted in `PrunedSchemaGuaranteeTest`.
- [x] **T077 — deferred by decision 68.** The Synthea corpus is the large-scale synthetic round trip, and it is deferred to a later phase rather than run in M1. `SyntheaRoundTripTest` and its vendored corpus stay in place so that reinstating it is a matter of re-enabling the class. T134d still records the wider corpus `benchmark` has and why it was not taken.
- [x] **T078 — withdrawn by decision 68.** There is no lexical form left to lose, because both ingest paths route a number through a double and therefore agree. `StringDatasetLimitationTest` is deleted and FR-020 is withdrawn with it.
- [x] T078a [P] [US2] **Reduced by decision 68**: test that on a pruned schema the round-trip guarantee holds subject to FR-016's exception list — which until M5 includes primitive id and extension content, per FR-017's carve-out — in `io/src/test/java/au/csiro/pathling/io/PrunedSchemaGuaranteeTest.java`, renamed from `DenseBoundsDetectionTest` for what it now covers. The dense-bounds half is withdrawn with T078b. The carve-out's own reporting is T057b; T078c closes it.
- [x] T078d [P] [US2] Round-trip through Parquet files on disk rather than in memory, in `io/src/test/java/au/csiro/pathling/io/PersistedRoundTripTest.java`, via the harness's `persistingTo`, which also asserts the schema read back is the schema written, less nullability. Every other round trip goes from the transform to the serialiser without touching storage, which proves those two and not the layout between them. A sample: the five specification examples with the most structural corners, and two Synthea types, all on a pruned schema. Added by M1's first review. **Amended by decision 69**: the dense arm this task also named is gone with the dense mode, which M6 now carries, so `PersistedRoundTripTest` has no dense case to assert bounds on.

### Implementation

- [x] **T078b — withdrawn by decision 68.** `BoundsCheck` is removed from M1 and parked as a follow-up (T134f). The bounds themselves stay, because they are what makes a dense schema over a cyclic definition graph finite (FR-044); only the reporting of what they drop goes, so dense-mode ingest now truncates silently. This is no regression against the current release, whose encoder also truncates silently at its nesting bound.
- [x] T079 [US2] **Amended by decision 68, relocated by decision 70**: implement egress from the layout to JSON in `io/src/main/java/au/csiro/pathling/io/egress/ResourceSerialiser.java`, since folded into `ResourceTransformer.toJsonShape` and `FhirJsonWriter`. The decimal branch becomes a cast of the stored text to `DoubleType`, so the writer emits a bare number, and the `DecimalTransform.unmarkedDocument` wrapper round the `to_json` is deleted with the marking machinery — which also removes a `regexp_replace` pass over every serialised document. The `base64Binary` branch is T060a's egress half.
- [x] **T080 — withdrawn by decision 71.** `EmptyPruning` is deleted. The omission it implemented grew the egress expression exponentially with nesting depth, and both the fix and FR-019 are deferred to the custom JSON serde the decimal's lexical form waits on (T134h). What egress keeps is one `isNotNull` on the stored structure per level, which is what stops an absent element being rebuilt as a present one, and is not pruning.
- [x] T080b [P] [US2] **New, from decision 71.** Round-trip a recursive element nested twelve levels deep, in `io/src/test/java/au/csiro/pathling/io/NestingDepthRoundTripTest.java`. No corpus in this repository reaches that depth — the specification examples and Synthea both stay in single figures — which is why 330 green tests said nothing about an expression that doubled per level. Measured at that depth: the withdrawn pruning dies with `Java heap space` in 29 seconds, and without it the document round-trips in 5.
- [x] T080c [P] [US2] **New, from decision 71's addendum.** Pin what M1 stores and writes where it does not prune, in `io/src/test/java/au/csiro/pathling/io/DeferredPruningTest.java`. The data-absent-reason document keeps its metadata-only `name` present in its slot, writes it as `{}` and reports the `_family` it does not store. The same `name` alone in its input is kept as well, and so is a metadata-only `contact.name`, per T080d. A repeating primitive keeps its positional nulls, `[Ann, null, Bee]` and `[null]`, and writes them without the `_given` they align with. A `photo` whose only content is a `size` that another document in the file re-typed comes back as `{}` in a document that is conformant on its own. No conformance suite carries any of these shapes (`evidence/null-and-empty-handling.md` §9), so a later change that pruned them, and changed query answers, would otherwise pass unnoticed.
- [x] T080d [US2] **New, from decision 72.** Store a primitive the source carried only as its metadata group as a null of the type the definitions give it, in the shape FHIR gives the group (an object beside a singular primitive, an array of objects beside a repeating one), so that the structure holding it is kept whatever other conformant documents are read with it, rather than omitted where nothing else read with it gave the element a column. In `ResourceTransformer`'s field planning, reusing the typed null already built for contradicted content. Flip the tests that pinned the omission (`ResourceTransformerTest`, and the fourth case of `DeferredPruningTest`, now with a repeating `_given`-only case, a nested `contact.name` case and a singular `Encounter.period` case beside it), and assert that a metadata group in any other shape keeps nothing (`ResourceTransformerTest`) and is reported as a shape mismatch rather than as primitive metadata, alone or where it re-types a conformant neighbour's group (`StrictnessTest`), and remove the metadata exclusion's cascade from `RoundTripHarness`, which existed only to hide the omission — `ActivityDefinition.timingTiming` in the specification's examples is the corpus instance, and now round-trips as `{}` on both sides. The released encoder agrees for a singular primitive and not for a repeating one, which T101 records.
- [x] T080a [US2] **New, from decision 70.** Split `io`'s public API into a transformer that is structured in and structured out and a reader and writer over JSON text. `ResourceTransformer` gains `toLayout` and `toJsonShape` and loses `read` and `source`; `ResourceSerialiser` is folded into it and `io.egress` into `io.transform`. `io/src/main/java/au/csiro/pathling/io/json/FhirJsonReader.java` reads files or a `Dataset<String>` with `FAILFAST`, and `FhirJsonWriter.java` writes a `Dataset<String>` or files, with `ignoreNullFields` on both. Widen the integral converters to accept `ByteType`, `ShortType` and `IntegerType`, and `decimal` to accept the same. Document the one-type-per-input constraint on both layers. Test the transform directly on datasets built with an explicit schema in `io/src/test/java/au/csiro/pathling/io/transform/StructFixtureTransformTest.java`, and the reader and writer in `io/src/test/java/au/csiro/pathling/io/json/`.

**Checkpoint**: Driver 2 is demonstrated over a real corpus, with no user-visible change.

---

## Phase 6: User Story 8 - The module boundary (Priority: P3)

**Goal**: The new encoding path carries no dependency on internal Spark Catalyst
API, and the existing implementation is untouched.

**Independent Test**: The build fails when the boundary is violated.

_Mostly delivered by T004–T009; this phase verifies it. It sits at the end of M1
because there is now encoding code for T126 and T127 to inspect._

- [x] T125 [P] [US8] Re-check the dependency ban against real code: add a Catalyst dependency to `fhir-schema/pom.xml`, confirm the build fails, and revert. T005 proved the rule on an empty module; this proves it still holds once the module carries derivation.
- [x] T126 [P] [US8] Re-check the import ban against real code: add a Catalyst import to the new encoding code, confirm the build fails, and revert. T007 proved the rule on an empty module. _Verified with the import **used**: spotless runs ahead of checkstyle and removes or reorders an unused one, so an idle import fails the build on formatting and never reaches the ban. The boundary holds either way, but only a used import proves the ban itself fires._
- [x] T127 [US8] Confirm by inspection that the new encoding path contains no expression encoder, no hand-authored serializer or deserializer expression tree and no FHIR object in a per-row plan (FR-050).
- [x] T128 [US8] Confirm `encoders` is unmodified — `git diff main -- encoders/` is empty — and that `mvn -pl encoders` still resolves (FR-051). T128a re-checks it under a narrowed assertion once the query-time toolkit has been extended.

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

_First in the milestone: these pin behaviour that Phase 8 changes, and they must
exist before the fixture mechanism moves underneath them._

- [ ] T020 [P] Add portable JSON fixtures carrying references between resources, in the same form as the existing `viewTests` fixtures, under `fhirpath/src/test/resources/viewTests/`: a reference that resolves, one whose target is absent, one to a resource type not present, a versioned reference, and a repeating reference element with several targets.
- [ ] T021 [P] Add view test cases for `resolve()` returning the referenced resource, yielding empty on an unresolvable reference, on an absent reference and on an absent resource type, and returning all targets of a repeating reference, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T022 [P] Add cases for `resolve()` composed with traversal and with `where()`, in `fhirpath/src/test/resources/viewTests/`.
- [ ] T023 [P] Add a case exercising resource-key production and reference-key production together with the join they feed, in `fhirpath/src/test/resources/viewTests/`. The primitives have coverage today; the join does not.
- [ ] T024 Convert the two tests using direct resource construction in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/ResolveFunctionDslTest.java` to the declarative model builder, and confirm the class no longer references the HAPI resolver factory.
- [ ] T025 Confirm every new case passes against unmodified behaviour. A failure is a defect to raise separately, not to fix here.
- [ ] T026 [P] Add a divergent-schema fixture under `fhirpath/src/test/resources/viewTests/` whose two files deliberately disagree on a leaf of a repeating element, with a view unnesting that element and projecting only that leaf.
- [ ] T027 Assert against the real engine that the divergent-schema fixture returns resources from both files (FR-036). This pins the unnesting constraint; if it fails, unnesting has been reshaped into a leaf-level read and rows are being lost silently. T027a extends it once tolerant traversal exists.

### The gate on how far the dispatch arms are built

_Moved here from Phase 13. It decides the coverage standard for the Phase 8
dispatch arms, so it cannot be settled two milestones after they are written.
Its wiring stays in Phase 13._

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

_Moved here from Phase 9. Tolerance of an absent field is a precondition for
moving any fixture, not a consequence of the fitted schema: the existing encoder
already omits fields past its nesting bound, so absence is a property of the
current layout too, and `FhirViewExtraTest` already carries two excluded cases
for it under issue #2625. Emitting the tolerant expression at every traversal
step is what closes that gap, so it belongs ahead of the test infrastructure
rather than two phases behind it._

_This block covers the **absence** axis only. It does not make a fixture safe to
move to the new layout, because a field present in a different shape is a
different problem, and that is Phase 8's dispatch._

#### Tests ⚠️ write first, confirm failing

- [ ] T101 [P] [US4] Test that traversal to an element the definitions describe but the schema lacks yields an empty collection, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/AbsentElementTest.java`. **Amended by decision 72**: this is where conformant input first answers differently from the released encoder, and the difference is on purpose. A structure whose only content was a repeating primitive's metadata — `name:[{"_given":[…]}]`, or `ActivityDefinition.timingTiming` holding only `_event` in the specification's examples — is kept by the layout, as FHIR says it exists, and dropped by the released encoder, because the parser it reads through discards an `_x` array with no `x` array beside it. So `name.count()` is 1 on the layout and 0 on the released encoder. A second difference follows, against the specification rather than the released encoder: fhirpath.js answers 2 for `name.given.count()` on `_given` with two entries, counting a value-less primitive per entry, while the layout stores `given` as a null array and answers 0 until M5; T061 decides the alignment that settles it. Assert the layout's answer and name it as the known divergence, so that a parity test over both layouts expects it rather than discovering it. A singular primitive's metadata agrees on both layouts.
- [ ] T102 [P] [US4] Test that traversal to an element the definitions do not describe still raises an error, in the same file.
- [ ] T103 [P] [US4] Test that selecting a choice variant absent from the schema yields empty, in the same file.
- [ ] T104 [P] [US4] Test that combining an absent element with a populated one succeeds — union, combination, conditional selection and comparison (FR-027), in the same file.
- [ ] T105 [P] [US4] Test that sibling column combination tolerates a bottom-typed complex element, in `fhirpath/src/test/java/au/csiro/pathling/projection/SiblingCombinationTest.java`. No _primitive_ is untyped under FR-055, but an absent complex element is the bottom type, and the recursive selection path computes an expected element type that now meets it where it previously met a statically empty collection.
- [ ] T106 [P] [US4] Test that a view column declaring a FHIR type produces an output column of that type over an absent element (FR-028), in `fhirpath/src/test/java/au/csiro/pathling/projection/ProjectedColumnTypeTest.java`.
- [ ] T107 [P] [US4] Test that a view column declaring no type over an absent _primitive_ element succeeds, carrying the type the definitions give the element (FR-030), and that deriving an output type still fails with a message naming the column, its path and the remedy for a column that genuinely carries no type information (FR-029), in the same file. An element absent from the schema must no longer reach that failure.

#### Implementation

- [ ] T110 [US4] Emit the tolerant traversal expression from T038e at **every** traversal step, replacing the direct field reference, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/DefaultRepresentation.java`, with the fallback type taken from the element definition per FR-055. It must be every step, not only where absence is suspected: traversal into a fallback fails for every possible fallback type, so the tolerance has to intercept before a direct field reference is ever emitted over one.
- [ ] T111 [US4] Apply the declared FHIR type as a cast on output, not only the explicit SQL-type tag, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T112 [US4] Improve the message raised when no type information is available to name the column, its path and the remedy, in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectedColumn.java`.
- [ ] T113 [US4] Ensure sibling combination tolerates a bottom-typed complex element in `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectionResult.java` and `fhirpath/src/main/java/au/csiro/pathling/projection/RepeatSelection.java`.

### Test infrastructure for the flag day

- [ ] T034 Make the JSON path with an explicit derived schema available to the YAML conformance runner's fixture factory, as the new-layout arm of the T037a dimension rather than as an unconditional switch, in `fhirpath/src/test/java/au/csiro/pathling/test/yaml/`.
- [ ] T035 Make the JSON path available to the SQL-on-FHIR view test pipeline as the new-layout arm of the T037a dimension, rather than switching it off parsing and encoding unconditionally, in `fhirpath/src/test/java/au/csiro/pathling/views/FhirViewTest.java`. The local fixtures and the submodule fixtures come along unchanged.
- [ ] T036 Make the category-C path — construct, serialise to JSON, read with the derived schema — available to the object-based test data source as the new-layout arm of the T037a dimension, so the fluent builders survive unchanged, in `fhirpath/src/test/java/au/csiro/pathling/test/`.
- [ ] T037 Add the schema mode switch to the test framework as a dimension. **Amended by decision 69**: the dense mode does not exist until M6, so the dimension is built here with pruned as its only value and T037b adds `-Dpathling.testSchemaMode=dense` beside the mode itself. The dimension is built now rather than with the mode because this is where the suite moves onto the new layout, and retrofitting a dimension onto a switched-over suite is the more expensive order. The default is pruned, which is safe here only because the whole absent-element block has already landed in this phase, not T110 alone: a fitted schema also needs the _typed_ empty output that T107, T111 and T113 provide, or a view over an absent element crashes in sibling combination rather than yielding a column. T038 is what stops the switch failing quietly.
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

_T083–T085 and T091–T093 (Coding by name) are correct under both layouts and can
land at any time, including during M1. The rest of this phase cannot: T086 to
T089 and T094 to T099 are the layout dispatch itself, and T089 runs the whole
suite over the new layout._

_Annotations are not emitted until M5, so every value here is computed rather
than read from an annotation. That is what FR-022 requires in any case; the
annotation fast paths are T086a, T087a, T095a and T096a, in M5._

_The dispatching expression must satisfy one rule, and it now holds without
qualification: **every branch yields the same `dataType`**, across layouts as
well as within one, because T094b normalises the previous layout to the new
layout's shape rather than carrying it forward. That is what keeps T108 and
FR-054 true. Decision 47 originally had to weaken this across layouts; decision
55 records why it no longer does._

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

_The absent-element work that used to sit here — T101 to T107 and T110 to T113 —
moved to Phase 7, because tolerance of an absent field has to precede any fixture
movement. What remains is the reconciliation half, which depends on T038l rather
than on T110._

_This phase also **fixes an existing defect**. Unification today is by implicit
cast only, so two collections of one complex FHIR type whose SQL shapes differ
are reported compatible and then compared as mismatched structs. Under the
previous layout that is reachable wherever a recursive type is met at two
nesting depths, which the encoder's bound makes structurally different. The
fitted schema widens the same hole to non-recursive types reached by different
branches. One mechanism closes both, which is why T104b pins the recursive case
against unmodified behaviour before the fix lands._

### Tests ⚠️ write first, confirm failing

- [ ] T104a [P] [US4] Test that two collections of the same FHIR type reached by different paths, whose fitted schemas differ, combine successfully — holding every element of both and remaining traversable (FR-056) — and that the result's fields are in definition order (FR-057), in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/operator/ShapeReconciliationTest.java`. Cover `combine`, `|`, equality of complex values, conditional selection and membership.
- [ ] T104b [P] [US4] Test that two collections of the **same recursive** FHIR type reached at different nesting depths compare and combine — a `Questionnaire.item` at one level against one at the level below — in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/operator/ShapeReconciliationTest.java`. **This fails on `main` today**, under the previous layout and a dense schema, because the encoder truncates the recursive expansion at its nesting bound: the two structs carry different depths and therefore different SQL types, while `convertibleTo` reduces to FHIR type equivalence and reports them compatible. So this task pins an existing defect rather than an accommodation for the fitted schema. Run it against unmodified behaviour first and record the failure, so the fix is demonstrable.
- [ ] T109 [US4] Run the shape-sensitive tests with an explicit expected schema assertion, since a pruned schema is derived from the whole fixture set and adding a fixture can silently flip an assertion from a null branch to a missing-field branch.

### Implementation

- [ ] T113a [US4] Implement **one variadic unification entry point** over the reconciliation expression from T038l, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/CombiningLogic.java`, and delete the `instanceof DecimalCollection` special case in `prepareArray` that it subsumes (FR-056). It promotes the FHIR type and then unifies the SQL shape, in that order. The two steps stay conceptually distinct, because `FhirPathBinaryOperator.reconcileTypes` answers a definition-driven question and shape unification answers a structural one, but they stop having **disjoint callers**: today `prepareArray` reconciles decimal precision for union and combine while `reconcileTypes` promotes types for equality, comparison and arithmetic, and no site does both. Variadic rather than binary because `traverseChoice` coalesces several variants at once; folding is T038k's subject, not a second entry point.
- [ ] T113b [US4] Route **every** site that needs two or more operands to share a type through T113a's entry point (FR-056): `SameTypeBinaryOperator`, and therefore equality and comparison, which today calls `reconcileTypes` alone and is the site T104a and T104b exercise; `MathOperator`; `UnionOperator` and `CombineOperator`; conditional selection and membership; and `ColumnRepresentation.traverseChoice`, which passes the whole ordered operand list rather than folding. Routing rather than enumerating is the point: a site added later that does not call the entry point fails as a Spark analysis error the engine cannot name, which is the failure mode the layout dispatch had before decision 55 collapsed it.

**Checkpoint**: Two fitted shapes of one FHIR type reconcile, so the public API
can safely write a schema fitted to the data.

## Phase 10: User Story 5 - Divergent files read as one dataset (Priority: P2)

**Goal**: Batches written at different times query as one dataset without loss.

**Independent Test**: Write two batches with divergent schemas; query across both
and assert every resource appears with correct cardinality.

_In M2 rather than M1 because its subject is batches arriving over time, and
because T116 depends on US4. It sits **before** the flip rather than after it:
divergent fitted schemas arise the moment the new layout is written, so reading
mixed-shape files has to work by Phase 12, not after. Until then the batches are
written through `io`'s own entry points rather than the public write path. The
merge it needs (T038g) landed in M1._

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

_The point of no return, and the only breaking release. It lands after US4
because until absent elements behave the public API would be writing a fitted
schema the engine cannot fully query, and beside US7 because a user whose data
predates the flip must meet a message rather than a wrong answer. The engine
already reads both layouts, so the build does not go red across the switch._

_T108 and T108a sit here rather than with the rest of US4: they assert the
expression-to-column guarantee through the public API, which has no fitted mode
until this phase. The semantics they rest on are covered in Phase 9 by T101–T107
and T104a, so FR-054 and SC-010 are proven end to end here and not before._

_The layout lands annotation-free and without primitive metadata. FR-022 makes
the first safe; FR-017's carve-out, asserted by T057b, bounds the second. Both
close in M5._

- [ ] T070 [US1] Wire the transform into `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signatures (FR-043). **Amended by decision 70**: `encode` calls `FhirJsonReader.read(resourceType, Dataset<String>)`, and first selects the documents of the requested type, because today's `encode` discards documents of other types and the reader's input carries one type.
- [ ] T081 [US2] Wire egress into the decode entry point in `library-api/src/main/java/au/csiro/pathling/library/PathlingContext.java`, preserving the existing signature. **Amended by decision 70**: `decode` is `FhirJsonWriter.write(resourceType, dataset)`.
- [ ] T082 [P] [US2] Wire egress into `library-api/src/main/java/au/csiro/pathling/library/io/sink/NdjsonSink.java`. **Amended by decision 70**: through `FhirJsonWriter.write(resourceType, dataset, path, mode)`, which writes files directly rather than a column of strings.
- [ ] T108 [P] [US4] Test that one column expression, built with no reference to any dataset, applied to two datasets holding the same resources, yields equal results for every expression in the engine's expression-level test set (FR-054, SC-010), in `library-api/src/test/java/au/csiro/pathling/library/FhirPathToColumnTest.java`. This is the assertion that the expression-to-column API's unchanged signature is honest. **Amended by decision 69**: the dense mode does not exist until M6, so the two datasets here are fitted schemas of differing width — one corpus populating elements the other does not — which exercises the same schema independence. The fitted-versus-dense form is T108b, and SC-010 is satisfied in two parts. It is not deferred whole, because the guarantee is wanted at the flip rather than two milestones after it.
- [ ] T108a [P] [US4] Test that a column over an absent primitive element reaching a caller through the expression-to-column API carries the element's type and can be written to Parquet (FR-030), in the same file.
- [ ] T100 [US3] Run the full FHIRPath suite, both YAML conformance baselines and the SQL-on-FHIR compliance suite over the new layout (SC-002); remove exclusion entries that have become obsolete rather than leaving them to self-report (SC-003). Among them are the two `FhirViewExtraTest` exclusions for issue #2625, which T110 closes. Confirm the engine reads the new layout on every path; it still retains the previous-layout reader, which FR-053 permits until T100e removes it in M7.
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

_Immediately before the public API switch, and not earlier. The detector rejects
the layout an earlier release wrote, which is the only layout anyone holds until
the flip. Shipping it in M1 would reject every existing user's data in a release
whose engine still reads that data exactly as intended. It must be in place by
Phase 12 and must not ship before it._

_It depends only on Setup, so it can be built at any point; it must not be wired
in before Phase 12._

_The gate this phase is built to, T049a, is settled in Phase 7. Its answer says
whether the boundary refuses or routes, and therefore what T039 to T048 assert._

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

_These sit with the flip rather than after it. T129 and T130 describe the layout
the public API has just started writing; landing them later would ship an
undocumented layout. T136 measures the flip itself, and because the layout is
annotation-free at this point it measures the computed paths at their most
expensive — which is what makes it the input that orders M5._

- [ ] T129 [P] Rewrite `site/docs/libraries/io/schema.md` from [contracts/storage-layout.md](contracts/storage-layout.md), including the fitted-schema statement, the unnesting caution for direct SQL consumers and the compatibility statement for existing files.
- [ ] T130 [P] **Amended by decision 68**: the decimal limitation clause goes, because there is no per-path limitation left, and the strictness option goes, because there is no switch. What must be documented instead is that a decimal is stored and returned numerically rather than lexically, that content this layout does not store is dropped with a warning and cannot be made to fail, and that a column whose inferred type contradicts the definitions is dropped for the whole file. Document the new options and the behaviour changes from [contracts/library-api.md](contracts/library-api.md) in `site/docs/libraries/`, including the decimal limitation on the string-dataset path, that a column expression remains valid over any conformant schema with absent primitives carrying their definition-derived type, and that a column over an absent _complex_ element is bottom-typed and so cannot be written to Parquet and is omitted in JSON.
- [ ] T136 Re-run the split benchmark and compare against the T003 baseline. Record encode, decode, planning and execution separately. No outcome is required (SC-007). Because nothing is annotated yet, this measures every annotated operation on its computed path, so it is what orders Phase 15: FR-033 in particular leaves a precomputed reference key to be added if joins prove to need one. Re-run after each annotation kind lands.

**Checkpoint**: The new layout is documented and its unaccelerated cost is on record.

---

# Milestone 5 — The gaps

The two gaps the flip deliberately left open: annotations, then primitive ids and
extensions. Each annotation kind is an independent increment.

## Phase 15: Annotations

**Goal**: Each annotation kind is emitted by the encoder and used by the engine
as a fast path, one kind at a time.

_Ordered by what T136 measured, not by this list. Every kind is independent: it
can ship on its own, and until it ships the engine computes the value instead.
FR-021 is satisfied when the last kind lands, and FR-023 with T090._

_Correctness is never at stake here. FR-022 makes an annotation a fast path and
nothing more, which is what Phase 8 already delivers and T089 already asserts._

### Tests ⚠️ write first, confirm failing

- [ ] T050a [P] Test that a decimal carries the numeric annotation beside its stored text, in `io/src/test/java/au/csiro/pathling/io/transform/DecimalTransformTest.java`. Split from T050, which pins the lexical form in M1.
- [ ] T053 [P] [US1] Test that dates carry range annotations reflecting the stated precision, in `io/src/test/java/au/csiro/pathling/io/annotation/DateRangeAnnotationTest.java`.
- [ ] T054 [P] [US1] Test that a quantity carries both canonical annotations, in `io/src/test/java/au/csiro/pathling/io/annotation/QuantityCanonicalAnnotationTest.java`: `__<field>_canonical` present at the specification's `DECIMAL(38,6)`, and `__<field>_canonical_exact` immediately after it, asserting on the second that quantities differing by orders of magnitude do not compare equal. Pin the order, since field order is part of the type (FR-057).
- [ ] T055 [P] [US1] Test that each annotation can be disabled individually and that disabling one does not affect the others, in `io/src/test/java/au/csiro/pathling/io/annotation/AnnotationToggleTest.java`.
- [ ] T086a [P] Test that decimal comparison, arithmetic and ordering take the numeric annotation as a fast path where the schema carries one, giving results equal to the computed path T086 pins, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/DecimalCollectionTest.java`.
- [ ] T087a [P] Test that cross-unit quantity comparison takes the `_canonical_exact` annotation as a fast path where present, giving results equal to the computed path T087 pins, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/collection/QuantityCollectionTest.java`.
- [ ] T090 Test that whether an annotation is used is decided from the schema rather than per row (FR-023), by asserting the generated plan differs between an annotated and an unannotated input, in `fhirpath/src/test/java/au/csiro/pathling/test/AnnotationFreeSuiteTest.java`.

### Implementation

- [ ] T063 [US1] Implement the annotation processor registry in `io/src/main/java/au/csiro/pathling/io/annotation/AnnotationProcessors.java`, with each processor individually enableable. **Amended by decision 69**: this is where `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaConfiguration.java` is recreated, T033c having deleted it in M1 for want of a field. It goes **beside** the existing `EncodingConfiguration` rather than on it — that class lives in `encoders`, which FR-051 forbids modifying and which FR-052's carve-out does not reach — and carries the per-annotation toggles. T033 adds the schema mode to it in M6. **Amended by decision 69**: state here what a dense schema does with an annotation column — whether every element the definitions describe carries one, or only those the data populates. The dense mode does not land until M6, so this is decided without being exercised, and it is a width decision rather than a detail: `evidence/merge-cost.md` measures a dense `Observation` at 1371 leaf columns, and an annotation per element multiplies that. T064 to T066 follow whatever is decided here.
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

_Both halves land here, past the flip. The engine cannot navigate primitive
metadata today and the previous layout cannot represent it at all, so nothing
regresses by deferring the reading. Deferring the writing is the deliberate cost
recorded in FR-017's carve-out and decision 49: until T078c, a warehouse written
by this release does not carry primitive id or extension content, and T057b is
what keeps that loss visible rather than silent._

### Tests ⚠️ write first, confirm failing

- [ ] T051 [P] [US1] Test that a primitive element's id and extensions are stored in the metadata group beside it, in `io/src/test/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransformTest.java`.
- [ ] T078c [US6] Re-assert FR-017 unconditionally: remove the primitive-metadata exclusion from the round-trip harness and from T076, T077 and T078a, and delete the carve-out from the spec. The cascade the exclusion carried — the removal of containers the exclusion itself empties — is already gone, removed by T080d under decision 72. The `contained` exclusion beside it is **not** part of this: FR-006 is permanent. This is what closes decision 49. **Amended by decision 69**: the carve-out being deleted asserts dense behaviour — the group "on a pruned schema does not appear and on a dense one is present and null" — and the dense mode does not exist until M6. State what the dense mode is to do with the metadata group rather than deleting the sentence that says it, so M6 implements a decision rather than inferring one.

- [ ] T120 [P] [US6] Test navigation to a primitive element's extensions, in `fhirpath/src/test/java/au/csiro/pathling/fhirpath/PrimitiveMetadataNavigationTest.java`.
- [ ] T121 [P] [US6] Test navigation to a primitive element's id, in the same file.
- [ ] T122 [P] [US6] Test that both yield empty where the source carried neither, in the same file.
- [ ] T123 [P] [US6] Add fixtures carrying primitive ids and extensions under `fhirpath/src/test/resources/viewTests/`.

### Implementation

- [ ] T061 [P] [US1] Implement the primitive metadata transform in `io/src/main/java/au/csiro/pathling/io/transform/PrimitiveMetadataTransform.java`. **Two things M1 left for this task**, both recorded and neither yet true: the strip that keeps the metadata group out of a fitted schema must come out (decision 60), and egress must stop dropping an array's null elements individually, because the mechanism is positional and a dropped null shifts the parallel array out of line (decision 63). Populating the group without the second writes positions that do not correspond. **Amended by decision 72**: a repeating primitive the source carried only as `_x` is stored in M1 as a null array, not as an array of positional nulls, so `_given:[{…},{…}]` alone leaves `given` null. The metadata group this task writes has two entries and the value array none; decide whether the value array is to carry one null per entry, as FHIR's JSON aligns them, and write it that way rather than beside a null. Two further things for this task, from M1's round 9 review: the shape check decision 72 applies to a metadata group looks only at its outer shape, so a group whose content is not FHIR (`{"bogus":1}`, `{"id":5}`) is reported as metadata and keeps its structure, and this task, which writes the group, is where its content is checked; and the canonical structure emits a metadata group beside `Element.id` and `Extension.url` (`_id`, `_url`), which FHIR does not give them, so `name:[{"_id":{…}}]` is reported as metadata and keeps `name`. Decide whether those groups exist before writing them.
- [ ] T124 [US6] Implement navigation to the metadata group through the sibling resolution added in T094, in `fhirpath/src/main/java/au/csiro/pathling/fhirpath/collection/Collection.java`.

**Checkpoint**: Driver 3 is delivered end to end — stored and readable — and the round trip is unconditional again.

---

# Milestone 6 — The dense schema

The second schema mode: a schema comprising every element the definitions
describe, bounded by configuration rather than by the data. It is additive — a
caller who leaves the options and the mode at their defaults sees nothing change
— and it is what makes FR-009, FR-010 and FR-044 true of the new layout.

It lands here rather than in M1 because with `SchemaBuilder` deleted its absence
leaves no second derivation behind, and after M5 rather than before it because M5
is where the layout's width is settled. Decision 69 records both, and the price:
from the flip until this milestone, `max_nesting_level`, `enable_extensions` and
`enabled_open_types` are accepted by the public API and bound nothing.

## Phase 17: The dense schema

**Goal**: The dense schema mode is available, produced by the same traversal as
the pruned one.

**Independent Test**: Transform one corpus in both modes and compare the two
stored schemas; assert the bounds are what the dense one stops at.

_T031a comes first — everything else needs a cursor. T030 and T108b are the two
parity assertions and depend on the mode being wired by T031e._

### Implementation

- [ ] T031a Extract the traversal strategy interface in `fhir-schema` and implement the bounded cursor over it, carrying the nesting depth, the extension switch and the enabled open types, so that it becomes the only place FR-044's bounds live. The interface is extracted here rather than in M1 because it is now derived from two implementations rather than guessed from one. It answers three questions at a node: the entries in canonical order, whether a bound admits a given entry, and the cursor beneath it. **Corrected by decision 69**: the extension switch and the open-type set cut *entries*, including primitive leaves, which a descent returning empty cannot express; only the nesting bound cuts descent. `fieldOrder()` on `utilities`' `CanonicalStructure` MUST still report the full order at a node, which `StructureMerge` depends on. And the bounded cursor MUST NOT be memoised: the nesting bound counts recurrences per type identity along the path, with a reference bounded at zero, so two paths reaching one type with different path counts give different subtrees — "(type, remaining depth)" is not a key that captures that. A dense schema is built once per transform, so there is nothing to gain. Its state MUST stay out of `DefinitionCanonicalStructure`'s shared memo.
- [ ] T031e Wire the second strategy into the traversal of T031b: where the observed schema lacks a child, the dense mode emits a typed null built structurally rather than an absent field — `lit(null).cast(leaf)` at a leaf, and the same construction recursively beneath a struct or a repeating element. A fallback that materialises a `StructType` for this case MUST NOT be used, because it reintroduces the second derivation and with it the drift FR-010 exists to prevent (decision 68). Confirm that FR-011's post-order prune applies unchanged in this mode — a bound cuts whole subtrees rather than emptying a struct — and assert it rather than assuming it: Parquet cannot write an empty group, so this is a requirement and not a tidiness.
- [ ] T033 **Moved from M1 by decision 69.** Add the schema mode to `fhir-schema/src/main/java/au/csiro/pathling/schema/SchemaConfiguration.java`, which T063 recreated in M5 for the annotation toggles. The nesting, extension and open-type options stay where they are and bound the dense mode only (FR-044); this is the milestone at which they bound something again. Thread the mode onto `PathlingContext`, as T070 did for the transform.

### Tests

- [ ] T033a **Moved from M1 by decision 69.** Test that the schema mode is readable from the configuration and carries its documented default of pruned, in `fhir-schema/src/test/java/au/csiro/pathling/schema/SchemaConfigurationTest.java`. T030 covers mode parity through the transform; it does not cover the configuration surface itself.
- [ ] T030 **Moved from M1 by decision 69.** Dense and pruned differ only in the traversal strategy, so parity is asserted by transforming the same data in both modes and comparing types, cardinality and field order for the branches both carry (FR-010). Assert T030a's other half here: a pruned structure's field order is a subsequence of the dense one (FR-057). Normalise nullability before comparing — there is no `nullable()` normaliser since `SchemaBuilder` went, and `lit(resourceType)` is non-nullable.
- [ ] T031d [P] **New, from decision 69.** Test that the bounded cursor terminates when descending a self-recursive type, stopping at its bound, and that a reference reached within a reference is not expanded. A cursor bug on a cyclic graph does not fail, it hangs. T031c is the unbounded half of this.
- [ ] T037b [P] **New, from decision 69.** Add `-Dpathling.testSchemaMode=dense` to the dimension T037 built, and run the curated subset of the FHIRPath suite, the conformance baselines and the SQL-on-FHIR compliance suite in it (SC-002). T038's loud assertion that the active mode is the requested one already covers the switch failing quietly.
- [ ] T108b [P] **New, from decision 69.** Assert T108's guarantee in its fitted-versus-dense form: one column expression, built with no reference to any dataset, applied to a fitted and to a dense dataset holding the same resources, yields equal results for every expression in the engine's expression-level test set (FR-054, SC-010). T108 asserts the same property at the flip across two fitted schemas of differing width; this completes it.

**Checkpoint**: Both schema modes are available from one traversal, the bounds
options bound something again, and FR-009, FR-010 and FR-044 are met.

---

# Milestone 7 — Completion

The previous-layout reader is removed, the superseded design is retired, and the
follow-ups are raised. Nothing user-visible.

## Phase 18: Completion

_T131, T132, T133, T134, T134a, T137 and T138 depend on nothing beyond M1 and may
land at any point; T131 in particular documents modules that already exist.
Documentation of the layout itself is not here — T129 and T130 ship with the flip
in Phase 14, because a layout users hold and cannot read about is worse than one
they do not hold yet._

_T100e is the only task here that removes behaviour, and its scope is set by
T049a: if the source boundary routes earlier-layout data rather than refusing it,
the previous-layout reader is a product feature and removing it is a breaking
change rather than a cleanup._

_No migration tooling is built by this programme. T042's remedy is re-import from
source, T134a raises the rewrite tool as a follow-up, and FR-051's retention of
`encoders` is what keeps it possible later._

### The gate on the end state

- [ ] T100f **Gate.** Settle FR-053's end-state wording before T100e runs. The provisional position is that FR-053 states the transitional permission explicitly — the engine MUST read the new layout and MAY retain a reader for the previous one until this phase, after which it reads only the new layout. Confirm or revise it, and reconcile it with whatever T049a settled.

### Tasks

- [ ] T131 [P] Update the module list and dependency diagram in `CONTRIBUTING.md`, and the build-order line in `.claude/CLAUDE.md`. May land as soon as M1 does; `fhir-schema`, `io` and the `utilities` additions already exist by then.
- [ ] T132 [P] File the upstream Spark issue as a follow-up to SPARK-48148, with the reproduction from `evidence/spark-type-findings.md`: exact string parsing covers only byte-array and positionally-readable content references, so the string-dataset and `from_json` paths still route numbers through a double.
- [ ] T133 [P] Raise the follow-up issue recording the two alternative bindings from R-017 as future options — evaluation-time schema binding, and an unbound column representation bound when the dataset is known. Note what each would add beyond what is built here: bind-time validation and a required-schema manifest, and therefore Pathling-level messages naming the missing element instead of a Spark analysis failure. Note also that they remain compatible, because the representation of absence and the reconciliation mechanism are shared. Typed absent elements are **no longer** a follow-up: FR-055 delivers them for primitives.
- [x] **T134 — withdrawn by decision 68.** The quoting pre-pass existed to preserve a lexical form that is no longer preserved on any path, so there is nothing for it to protect.
- [ ] T134a [P] Raise the follow-up issue for the data-at-rest rewrite tool that T042's message says does not yet exist. Record that the retained `encoders` module is what keeps it possible, that a rewrite cannot restore any decimal precision the previous layout's fixed-precision numeric dropped (note that decision 68 means the new layout does not preserve lexical form either, so the gap between a rewritten and a re-imported warehouse is now narrower than it was), and that a re-imported warehouse therefore carries the losslessness guarantee where a rewritten one does not. This closes the gap between the Constitution Check's retention rationale and work that appears nowhere in this programme.
- [ ] T134b [P] Raise the follow-up issue tracking the server's move onto the post-change line. The assumptions in `spec.md` pin the server to the last library release before Phase 12 and require it to reach the post-change line before the pinned line stops receiving fixes. That obligation has a deadline and no owner anywhere in this programme, which is what this task fixes.
- [ ] T134c [P] Raise the follow-up issue for the object-storage half of the T119 merge gate. R-015's condition was _narrowed_, not discharged: `evidence/merge-cost.md` establishes the shape of the curve locally and shows Delta does not bite, but no object-storage harness exists in this repository, so the wall clock a user on S3 would see for raw files is still unmeasured. Record that the fallback R-015 contemplates therefore remains open rather than ruled out.
- [ ] T134d [P] Raise the follow-up issue for the round-trip corpus that `benchmark` already resolves. `au.csiro.pathling:test-data` ships 25 per-resource-type NDJSON files under `bulk/fhir/` on the classpath, resolved from Maven Central and consumed by `benchmark/src/main/java/au/csiro/pathling/benchmark/BenchmarkResources.java`. T077 instead runs over four resource types vendored into `io/src/test/resources/data/synthea/R4/`, so the wider corpus is available and unused. It was left out of M1 deliberately: `io` is in the `library-runtime` chain and `benchmark` is not, so the dependency adds a 124 MB test-scope download to the core build that a fresh checkout does not pay today, and the cost of round-tripping 600 MB on every build is unmeasured. The follow-up decides whether to take the dependency, and whether to bound what the harness reads per resource type rather than consuming all of it. It widens the resource-type spread only; it is not a substitute for T076, because Synthea does not emit the structural corners the specification examples exist to demonstrate.
- [ ] T134f [P] **New, from decision 68.** Raise the follow-up issue for dense-mode bounds reporting, which `BoundsCheck` provided and M1 no longer does. Record that the bounds themselves are unchanged (FR-044), that only the reporting went, that dense-mode ingest therefore truncates silently as the previous encoder did, and that decision 59's separation of a bound from undescribed content should be reinstated with the check rather than reasoned out again. **Amended by decision 69**: with the dense mode landing in M6, immediately before this phase, the check can be built there instead of raised here. Decide which, rather than raising a follow-up for work that is one milestone old.
- [ ] T134g [P] **New, from decision 68.** Raise the follow-up issue for the strictness switch and value validation. Record what M1 gave up: a failing mode for content the definitions do not describe, value-range validation such as `positiveInt` of `0`, and per-value rather than per-column granularity, so that one non-conformant value voids its element for a whole file. Record also that decision 57's analysis of why a plain cast cannot be relied upon — ANSI mode being the caller's, the cast accepting `yes`, a decimal no cast sees — remains correct and should be the starting point rather than rediscovered.
- [ ] T134e [P] Raise the follow-up issue for the inline resource that is dropped for want of a type, per decision 64. `DefinitionCanonicalStructure.addElement` omits an element whose `ElementDefinition.getFhirType()` is empty, which is every element declared as the abstract `Resource`, and `StrictnessCheck` then reports the observed content as `undescribedContent` — "the definitions describe no element of this name" — for an element the definitions plainly describe. The outcome is right and the diagnosis is wrong. Record that `contained` is handled correctly by name while `Parameters.parameter.resource` is not, that `Bundle.entry.resource` takes the same branch and so M3's explode path (T058, T068) will meet it, and that the likely remedy is a fourth `NonConformantContent` kind naming an inline resource rather than widening `containedResource`. Found by T076 over the specification's own examples. M1's first review found a second mis-diagnosis of the same shape, to be raised in the same issue: an empty array (`"identifier":[]`) is inferred with a `NullType` element, and `StrictnessCheck.checkElement` reports it as "the definitions declare a structure and the source carries a value" when the source carries no value at all. The input is non-conformant either way, so again only the diagnosis is wrong.
- [ ] T134h [P] **New, from decision 71.** Raise the follow-up issue for the custom JSON serde, carrying both the decimal's lexical form and the empty pruning. Record that the two share one cause — the document is parsed and rendered by Spark's JSON reader and writer, which route a number through a double and render whatever structure they are given — and that a serde of our own decides both at once. Record what M1 gives up meanwhile, as decision 71's addendum narrows it. A structure emptied by non-conformant content is kept in the layout and written as `{}`, which covers a conformant document whose column another document re-typed; layout data from another producer can carry the same, or `[]`; and a source `[]` or `{}`, which is not conformant, is stored and written back verbatim with no finding wherever another document in the file types the column. The document is then not conformant FHIR, and the engine's `exists()` answers true for an element that holds nothing. The serde should prune these. It must not prune a structure whose only content was primitive metadata, which is also written as `{}` in M1 but is an element that exists, so `exists()` is right to count it; nor compact a repeating primitive's positional nulls. M5 gives both their `_x`, which is what makes their output conformant. Record the fallback if the serde does not happen, so that it is not rediscovered: the emptiness test is recursive and data-dependent, but its predicate must be built from the stored leaves rather than from the values it guards, with `exists` over an array rather than `size` over a built one; and better, ingest can carry the invariant as FR-011 already says of the schema, leaving egress a single `isNotNull`. Raise it with T132 and T135, which are the decimal's other two threads.
- [ ] T135 Benchmark the two ingest mechanisms against each other — the chosen transform approach and direct parsing into a variant. Note that decision 68 removes one of approach (b)'s advantages, since losslessness on every route no longer distinguishes it — and record the comparison in `evidence/ingest-comparison.md`. This records evidence for a future release rather than deciding anything here: the mechanism is settled by this point, so the lexical-decimal limitation stands for this programme, pinned by T078 and documented per FR-020 (R-009).
- [x] T137 Retire the earlier draft design, which this specification supersedes. It was never committed.
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

| Requirement                                                                                                          | Tasks                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| FR-001 Layout conformance                                                                                            | T050, T052, T056, T057, T057a, T057b, T057e, T059, T060, T062, T067, T067a (M1), T058, T058a, T068, T069, T069a (M3), T070 (M4), T051, T053–T055, T061, T063–T066 (M5)                    |
| FR-002 Decimals as text plus annotation (lexical form withdrawn, decision 68)                                         | T050, T060 (M1), T050a, T064 (M5)                                                                                  |
| FR-003 Primitive metadata groups, inline extensions, no identifier or map                                            | T052, T062 (M1), T051, T061 (M5)                                                                                                                                                   |
| FR-004 Date range annotations                                                                                        | T053, T065 (M5)                                                                                                                                                                    |
| FR-005 Both quantity canonical annotations, the specification's and a magnitude-preserving one                       | T054, T066 (M5)                                                                                                                                                                    |
| FR-006 `contained` excluded and reported (governance by switch withdrawn, decision 68)                                | T056, T067                                                                                                         |
| FR-007 `Bundle` never stored                                                                                         | T058, T068                                                                                                                                                                         |
| FR-008 Schema derived from definitions                                                                               | T028, T031                                                                                                                                                                         |
| FR-009 Pruned and dense modes (the dense mode is delivered in M6, decision 69)                                        | T029 (M1, the pruned mode); T030, T031a, T031e, T033, T033a (M6)                                                    |
| FR-010 Dense and pruned are one traversal, differing only in strategy (decision 68; met when M6 lands, decision 69)   | T031b (M1, the traversal); T030, T031a, T031e (M6)                                                                  |
| FR-011 No field-less structure                                                                                       | T029, T080d                                                                                                                                                                        |
| FR-012 Types and cardinality never from data                                                                         | T028                                                                                                                                                                               |
| FR-013 Child enumeration                                                                                             | T013, T016                                                                                                                                                                         |
| FR-014 Cardinality exposed                                                                                           | T014, T017                                                                                                                                                                         |
| FR-015 Withdrawn (decision 67)                                                                                       | —                                                                                                                                                                                  |
| FR-016 Round-trip equality, numerically, subject to the stated exceptions (decision 68)                               | T071, T076, T078d, T080c                                                                                           |
| FR-017 Pruned subject to FR-016's exceptions; dense bounded (detectability withdrawn, decision 68)                    | T057b, T067a, T078a (M1, under the primitive-metadata carve-out); T078c (M5, removing it); the dense clause with T030 (M6) |
| FR-018 *Withdrawn for this phase by decision 68*; ignoring is the only mode and is reported                           | T057, T067, T134g (the follow-up)                                                                                  |
| FR-019 *Deferred by decision 71*; a null field is left out, an empty structure or array is not                       | T080a, T080b, T080c, T134h (the follow-up)                                                                                                                                         |
| FR-020 *Withdrawn by decision 68*; no lexical form is preserved on any path                                           | —                                                                                                                  |
| FR-021 Annotations default on, individually disableable                                                              | T055, T063 (M5)                                                                                                                                                                    |
| FR-022 Engine computes when annotations absent                                                                       | T089                                                                                                                                                                               |
| FR-023 Fast path chosen from the schema                                                                              | T090 (M5)                                                                                                                                                                          |
| FR-024 Absent element yields empty                                                                                   | T101, T110, T110a                                                                                                                                                                  |
| FR-025 Undefined element raises                                                                                      | T102                                                                                                                                                                               |
| FR-026 Absent choice variant yields empty                                                                            | T103                                                                                                                                                                               |
| FR-027 Absent combines with populated                                                                                | T104, T105, T113                                                                                                                                                                   |
| FR-028 Declared FHIR type applied to output                                                                          | T106, T111                                                                                                                                                                         |
| FR-029 Untyped derivation fails with an actionable message, and an absent element is not such a column               | T107, T112                                                                                                                                                                         |
| FR-030 An absent primitive carries its definition-derived type                                                       | T107, T108a, T130                                                                                                                                                                  |
| FR-031 Coding decoded by name                                                                                        | T083, T084, T091                                                                                                                                                                   |
| FR-032 Terminology on narrowed Coding                                                                                | T085, T092, T093                                                                                                                                                                   |
| FR-033 Reference keys without a stored column                                                                        | T088, T098, T094b                                                                                                                                                                  |
| FR-034 Primitive id and extension navigation                                                                         | T120–T124 (M5)                                                                                                                                                                     |
| FR-035 Query-time decimal precision unchanged                                                                        | T086, T094b, T095, T130, T086a, T095a                                                                                                                                              |
| FR-036 Unnesting never reduced to a single leaf                                                                      | T026, T027, T116                                                                                                                                                                   |
| FR-054 Traversal tolerant of an absent field, decided after schema resolution                                        | T038a, T038b, T038d, T038e, T110, T110a (M2, all in Phase 7), T108 (M4)                                                                                                            |
| FR-055 Absent elements typed by the definitions or the bottom type                                                   | T038c, T038e, T105, T110                                                                                                                                                           |
| FR-056 Reconciliation by by-name projection, never by cast                                                           | T038j, T038k, T038l, T104a, T104b, T113a, T113b                                                                                                                                    |
| FR-057 Canonical field order, covering the layout's own fields, wherever a structure type is produced                | T030a, T038h, T038i, T104a, T118a                                                                                                                                                  |
| FR-058 One recursive field-wise merge, over a navigable canonical structure, serving reconciliation and file merging | T038f, T038g, T038h, T038i, T118a                                                                                                                                                  |
| FR-037 Unsupported layouts rejected with an actionable message                                                       | T042, T045, T046, T047 (read path only; the write path is FR-037a)                                                                                                                 |
| FR-037a Unsupported layouts rejected on the write path                                                               | T039a, T042a, T046a                                                                                                                                                                |
| FR-038 Detection structural, no extra file access                                                                    | T044                                                                                                                                                                               |
| FR-039 Sparse and unclassifiable schemas accepted                                                                    | T041                                                                                                                                                                               |
| FR-040 Detection disableable per source                                                                              | T043, T048, T049                                                                                                                                                                   |
| FR-041 Divergent files read as one dataset                                                                           | T114, T118, T118b, T118c, T119                                                                                                                                                     |
| FR-042 Divergent append widens the table                                                                             | T115, T117, T117a                                                                                                                                                                  |
| FR-043 Encoding and decoding APIs preserved                                                                          | T070, T081, T080a (M1 provides the `Dataset<String>` entry points they call, decision 70)                                                                                         |
| FR-044 Bounds options apply to dense only (inert from the flip until M6, decision 69)                                | T031a, T033 (M6)                                                                                                                                                                   |
| FR-045 XML and Bundle ingest preserved                                                                               | T058, T058a, T068, T069, T069a                                                                                                                                                     |
| FR-046 Earlier layouts not read as the new one                                                                       | T049a (settled in Phase 7), T042, T045                                                                                                                                             |
| FR-047 Published layout contract replaced                                                                            | T129                                                                                                                                                                               |
| FR-048 Dependency ban on the schema module                                                                           | T005, T125                                                                                                                                                                         |
| FR-049 Import check on the new encoding                                                                              | T007, T126                                                                                                                                                                         |
| FR-050 No encoder or hand-authored expression trees                                                                  | T127                                                                                                                                                                               |
| FR-051 Existing implementation untouched                                                                             | T128, T128a                                                                                                                                                                        |
| FR-052 Query toolkit preserved and not relocated                                                                     | T038e, T038l, T128, T128a                                                                                                                                                          |
| FR-053 Engine reads only the new layout                                                                              | T100f, T100e (M7), T038m, T094b. T100 confirms the new layout is read on every path but **does not** assert the previous reader is gone; the engine reads both from M2 until T100e |

And the success criteria:

| Criterion                                                                        | Tasks                                                                                  | Quickstart     |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------- |
| SC-001 Round trip over a reduced corpus, including the five decimal forms, compared numerically | T072, T076                                                                             | QS-001         |
| SC-002 Suites pass over the new layout in pruned mode, dense subset green        | T100, T037, T037a, T038, T100g (M4); T037b (M6, the dense subset)                      | QS-003         |
| SC-003 Exclusion baselines gain no entries, obsolete ones removed                | T100                                                                                   | QS-003         |
| SC-004 Suite passes with every annotation disabled                               | T089                                                                                   | QS-004         |
| SC-005 Divergent files return every resource with correct cardinality            | T114, T115, T116                                                                       | QS-006         |
| SC-006 Earlier layouts rejected at read time with an actionable message          | T042, T045–T047                                                                        | QS-008         |
| Follow-ups with a deadline and no other owner                                    | T134b (the server's move), T134c (the unmeasured object-storage half of the T119 gate) | —              |
| SC-007 Encode, decode, planning and execution measured separately                | T002, T003, T136                                                                       | QS-010         |
| SC-008 No incompatible public API change in Java, Python or R                    | T049, T070, T081, T082, T100b                                                          | QS-002, QS-008 |
| SC-009 Build fails on internal Catalyst API in the schema module                 | T005, T125, T126                                                                       | QS-009         |
| SC-010 One column expression gives equal results on a fitted and a dense dataset | T038a, T038b (M2), T108 (M4, across two fitted widths), T108b (M6, the dense arm)      | QS-005         |

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
- **M6** depends on M1 for the traversal it adds a strategy to, and on M5 by
  placement rather than by need: nothing in the dense mode requires an annotation
  or a metadata group, but building it after them means building it once, against
  the layout's final width (decision 69).
- **M7** depends on M5 for T078c's closure of FR-017, and on T049a for T100e's
  scope. It does not depend on M6, except that T134f's follow-up may be closed
  there rather than raised here.

### Within M1

- Phase 2 (definitions): T010–T012 (motion) before T013–T019 (widening).
- Phase 3 (derivation): depends on T016–T018. T038f and T038g are pure structure
  mechanics over `spark-sql-api` types and may run alongside; T038h and T038i need
  the definition widening, because the canonical structure is read from the
  definitions. Under decision 68, T031b is a walk of the structure T038i
  establishes and depends on it; under decision 69 it walks it directly rather
  than through an interface, which T031a extracts in M6. T031c follows T038i.
  T033c depends on nothing and may run at any point in the phase. T119 depends on
  T031b, the derivation having moved there from T031.
- Phase 4 (US1): depends on Phase 3. T057b and T067a depend on T067. T060b's
  converter table and T060a's `base64Binary` entry precede T057d, which tests
  them.
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

### Within M5, M6 and M7

- Phase 15: each annotation kind is independent of the others **after the
  first**. T063's registry and T055's toggle test are shared, so they ride with
  whichever kind ships first. The emission task
  precedes its fast-path task in every case. T090 depends on any one kind having
  landed, since it needs an annotated input to compare against.
- Phase 16 depends on T094, from Phase 8, and T078c depends on T051 and T061.
- Phase 17: T031a first, since everything else needs a cursor; T031e depends on it
  and on T031b; T030, T037b and T108b depend on T031e, the mode having to be
  reachable before parity can be asserted; T033 and T033a are independent of the
  cursor and may run alongside it.
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

**Conventions.** A field present in a _different shape_ is a different problem
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
them in M7.

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

**M6 adds the second schema mode**, against a layout whose width is by then
settled. One bounded cursor, the strategy interface extracted with both
implementations in hand, and the three bounds options bounding something again.
Additive, so nothing changes for a caller who leaves them at their defaults.

**M7 retires the debt.** The previous-layout reader and its dispatch arms come
out, FR-053 reaches its end state, and the superseded design is retired.

The server pins to the last library release before Phase 12 — the first change
users can observe — and must reach the post-change line before the pinned line
stops receiving fixes.
