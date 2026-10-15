# 05 — Remove the legacy-layout normalisation, its test dimension and CI job

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-046; decision 132. Old tasks: T100e and the removal of T100i's job. T049a and T100f are settled by ADR-0001.

**What to build:** The engine reads only the PoF layout. Every branch that converts the legacy layout at traversal goes, along with the machinery that exists only to test it:

- `DecimalNormalisation`, `QuantityNormalisation` and `InstantNormalisation`.
- The `_fid` and root extension-map arms of `TraverseExtension`. Remove the legacy arm first; the
  tolerant `_extension` and `_fid` table-column references in `ExtensionTraversal` (decision 79)
  then have no purpose, and go in the `_fid` commit below.
- The decimal arms of `ResolveOrNull`.
- The `*ColumnOrNull` helpers that serve only the legacy layout, and `readField`.
- The `pathling.testLayout` test dimension and every test arm that runs both layouts.
- The SpEL-scoped conformance exclusions that apply only to the legacy layout.
- The `test-fhirpath-previous-layout` CI job, in both `test.yml` and `pre-release.yml`.
- `_fid` from everything the engine produces, and the tolerance from decision 79 that exists only
  because of it. This is task T124k, brought into this ticket. It is a separate commit, because it
  changes the public shape of a System.Coding from seven fields to six:
  - `CodingSchema.FID_FIELD`, the `_fid` field in `codingStructType()`, and the null `_fid` in
    `encode()` and `toStruct`.
  - The null `_fid` that `CodingCollection` and `QuantityCollection` add to the structures they
    build.
  - The tolerant `_extension` and `_fid` table-column references in `ExtensionTraversal` (see the
    `TraverseExtension` bullet above).
  - `_fid` in `CodingDecoder.CODING_FIELDS` and its javadoc.
  - The `LayoutDetector` and `LayoutDetection` justification that rests on the `_fid` of a coding
    a terminology function returned. **Detection does not change** (owner, 2026-10-15): the root
    `id_versioned` stays the only marker that classifies a schema as the legacy layout, and the
    other legacy-layout markers stay evidence cited in a refusal, as decision 100 left them and
    decision 132's addendum relies on. Rewrite the Javadoc that justifies this, which now cites the
    terminology codings, to rest on the root versioned id being present whether or not the legacy
    encoder stored extensions, and on the other markers adding misclassification risk without
    adding cases caught. Keep `newLayoutSchemaWithOneNestedPreviousLayoutFieldIsAccepted` and the
    other detector tests passing, with names or comments updated only where they cite the
    terminology codings.
  - The `_fid` columns in `lib/python/tests/test_udfs.py` and the R `coding_row` helper.
  - **Approval gate for changed expectations.** Where removing `_fid` changes an existing test's
    expected value, other than a struct type or schema that lists `_fid`, stop and report each
    one, with its old and new value. Update them only after the owner approves.
  - Document the change: the six-field System.Coding in `contracts/engine-semantics.md`, beside the
    System.Quantity shape ticket 03 documented, and the change from seven fields in the behaviour
    changes of `contracts/library-api.md`.
  - Note on decision 79 and decision 100 in `evidence/decisions.md` that this ticket supersedes
    decision 79's tolerance and decision 100's reason for keeping only the root versioned id
    decisive, though not that rule itself.

Before deleting `DecimalNormalisation`, move `SCALE_SUFFIX` to where `LayoutDetector` can still use it.

The tests with a dual-layout arm, found by ticket 02's search, are below. Each one pins
`TestLayout.PREVIOUS` beside its PoF arm, so it encodes through `FhirEncoders` on every run. Paths
are relative to `fhirpath/src/test/java/au/csiro/pathling/`.

- `test/layout/TestLayoutSwitchTest`, which tests the switch itself.
- `search/SelfJoinAmbiguityTest`
- `search/JoinConditionTest`
- `search/QuantitySearchPrecisionTest`, whose expected rows at lines 100 to 106 hold only for the
  previous layout.
- `fhirpath/AbsentCodingTerminologyTest`
- `fhirpath/AbsentReferenceTypeTest`
- `fhirpath/ChoiceTypeInfoTest`
- `fhirpath/IndexedQuantityExtensionTest`
- `fhirpath/QuantityExtensionThroughProjectionTest`
- `fhirpath/StoredQuantityTest`
- `fhirpath/collection/ExtensionTraversalTest`
- `fhirpath/collection/CodingCollectionTest`
- `fhirpath/collection/DecimalCollectionTest`
- `fhirpath/collection/QuantityCollectionTest`
- `fhirpath/collection/QuantityTypesTest`
- `fhirpath/operator/CombiningPlanSizeTest`
- `fhirpath/operator/ShapeReconciliationTest`, which also builds its own nesting-bounded encoders
  for the previous-layout Questionnaires.
- `fhirpath/dsl/JoinKeyFunctionsDslTest`, which asserts that the previous layout stores
  `id_versioned`.

Several other tests encode through `FhirEncoders` only when the dimension selects the previous
layout, through `ObjectDataSource`, `LayoutDatasets` or the YAML resolver factories. Removing the
dimension removes that path for them; ticket 02's comments list them. `PofDatasets` is the PoF-only
replacement for `LayoutDatasets`.

**No check replaces them.** Under decision 132 the engine makes no check for the legacy layout
beyond detection, and ticket 04 was dropped. Data that bypasses detection is unsupported, so do not
add a check in place of what is removed, including for the root `_extension` map: an extension
traversal over legacy data answering empty after this ticket is accepted.

`fhirpath/.../search/filter/ElementMatcherTest` hand-builds quantities in the legacy shape, with a
decimal value and the canonical fields. Convert them to the PoF shape.

**Out of scope:** Distinguishing `System.Coding` from `FHIR.Coding` in the type system stays with
#2804; only the `_fid` field goes here. The encoders' own `_fid` (`SchemaConverter`,
`SerializerBuilder`, `DeserializerBuilder`, `ExtensionSupport`), which `FhirEncoders` and the legacy
export still need, stays. The server's own `_fid` handling, such as in `ExportExecutor`, stays: the
server is pinned to the last release before the PoF layout, and its move is #2837.

**Blocked by:** 02, 03

**Status:** ready-for-agent

- [ ] None of the listed classes, arms, the property or the CI job remain. `LayoutDetector` still compiles and detects the legacy layout.
- [ ] No `_fid` field is produced by the engine, `CodingSchema` or a terminology function, and no decision 79 tolerance remains. It is a separate commit.
- [ ] Detection is unchanged: only the root `id_versioned` classifies a schema as the legacy layout, and the detector tests pass, with the justification rewritten.
- [ ] Changed test expectations beyond the struct types were approved by the owner before they were updated.
- [ ] The six-field System.Coding is documented in `contracts/engine-semantics.md` and `contracts/library-api.md`, and decisions 79 and 100 carry a note that this ticket supersedes them as stated.
- [ ] The conformance baselines gain no entries. Exclusions made obsolete by the removal are taken out (SC-003, per the `pathling-yaml-exclusions` skill).
- [ ] The core build passes. The Python and R suites pass after the Ivy cache is cleared.
