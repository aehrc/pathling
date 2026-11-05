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

**Status:** done

- [x] None of the listed classes, arms, the property or the CI job remain. `LayoutDetector` still compiles and detects the legacy layout.
- [x] No `_fid` field is produced by the engine, `CodingSchema` or a terminology function, and no decision 79 tolerance remains. It is a separate commit.
- [x] Detection is unchanged: only the root `id_versioned` classifies a schema as the legacy layout, and the detector tests pass, with the justification rewritten.
- [x] Changed test expectations beyond the struct types were approved by the owner before they were updated.
- [x] The six-field System.Coding is documented in `contracts/engine-semantics.md` and `contracts/library-api.md`, and decisions 79 and 100 carry a note that this ticket supersedes them as stated.
- [x] The conformance baselines gain no entries. Exclusions made obsolete by the removal are taken out (SC-003, per the `pathling-yaml-exclusions` skill).
- [x] The core build passes. The Python and R suites pass after the Ivy cache is cleared.

## Comments

### Implementation (branch `issue/2829-remove-legacy-normalisation`)

Five commits:

- `5970b8834c` moves `SCALE_SUFFIX` to `DecimalCustomCoder`'s companion object, beside the encoder
  that writes the scale companion. `LayoutDetector` reads it from there.
- `b7cc10d7d1` removes the test dimension.
- `6d5e0d7f34` removes the normalisation.
- `b39fdeaed4` is T124k, the `_fid` removal, as a commit of its own.
- `6bc595b2f5` applies the clear-cut review findings.

**Removed.**
- `DecimalNormalisation`, `QuantityNormalisation`, `InstantNormalisation` and
  `InstantNormalisationTest`.
- The decimal arms of `ResolveOrNull` and its call to the quantity and instant normalisation.
- `decimalColumnOrNull`, `quantityColumnOrNull`, `instantColumnOrNull` and
  `ResourceRepresentation.readField`. A top-level field is now read with `getField`.
- In `TraverseExtension`:
  - `ExtensionLookup`, `UnresolvedTraverseRootExtension`, `UnresolvedTraverseRootExtensionByFid`
    and `fromRootFid`.
  - The `_fid` arms of `fromElement`.
  - Root extension traversal is now the tolerant reference to the `extension` column, falling back
    to the absent value.
- `TestLayout`, `LayoutDatasets` and `TestLayoutSwitchTest`, together with the layout half of
  `TestDimensionsTest`. `TestSchemaMode` and `DimensionProperty` stay.
- The `pathling.testLayout` property and its two `systemPropertyVariables` in `fhirpath/pom.xml`.
- The previous-layout arm of each of the 18 listed tests. `ObjectDataSource`, the three YAML
  resolver factories, `YamlSupport`, `FhirViewTest`, `AnsiTypeHintingTest` and
  `AnnotationFreeSuiteTest` now build their fixtures through `PofDatasets` only. `FhirViewTest`
  loses its nesting-bounded `FhirEncoders` bean, which only the previous layout used.
- `ShapeReconciliationTest` loses its nesting-bounded encoders. Its item cases still find two
  shapes for an item at two depths, because the innermost item has no items of its own.
- The `test-fhirpath-previous-layout` job, in both `test.yml` and `pre-release.yml`. It is also
  gone from `pre-release`'s `needs`, and from the sonar job's `needs`, its `if` and the comment
  above it.

**The order of the `TraverseExtension` change.** The ticket asks for the legacy arm to go first,
and for the tolerant `_extension` and `_fid` references to go in the `_fid` commit.
- The references were the inputs of the legacy lookup, so there was nothing left for them to
  refer to once the lookup went. They went with it in the normalisation commit.
- What stayed until the `_fid` commit is decision 79's tolerance for engine-built structures: an
  explicit arm answering the absent value for a structure carrying `_fid`. It went with the last
  engine-built `_fid`.
- The arm answered as the default branch does, so the split changes no answer at either commit.

**No check replaces the removed code** (decision 132). An extension traversal over legacy data,
whether at the root or under an element, now answers empty.

**`ElementMatcherTest`.**
- Its quantities are now in the PoF shape: the value as text, then `unit`, `system` and `code`.
  The `_value_canonicalized` and `_code_canonicalized` fields are gone.
- The value-only fixture's value is now text too.
- Every expected value is unchanged, and all 333 cases pass.

**System.Coding is six fields.**

```
struct<id: string, system: string, version: string, code: string, display: string, userSelected: boolean>
```

- This is the shape of `CodingSchema.DATA_TYPE`, of a Coding literal, of a terminology result and
  of the Coding that `QuantityCollection.toCoding` builds from a quantity's unit.
- `SystemCodingShapeTest` pins it against a struct written out in the test. It covers four
  producers: a literal, a literal with every field, a union of two literals, and `translate()`.
  It also checks the canonical struct type.
- The test compares field names and types, ignoring nullability, because a literal's populated
  fields are not nullable.
- A union of a stored Coding with a literal is reconciled by name and keeps the stored fields
  (decision 79's addendum). For that case the test checks only that `_fid` is absent.
- The test was run red first, with 7 of 7 failing on the trailing `_fid` field (log
  `coding-shape-red.log`).
- `_fid` is gone from:
  - `CodingSchema`, with its `FID_FIELD`;
  - `CodingCollection`, `QuantityCollection` and `CodingDecoder.CODING_FIELDS`, with the Javadoc of
    each;
  - `DefaultYamlTestExecutor`;
  - the Python `CODING_TYPE` and `CodingRow`, and the R `coding_row`.
- The encoders' own `_fid` (`SchemaConverter`, `SerializerBuilder`, `DeserializerBuilder`,
  `ExtensionSupport`) and the server are untouched.

**Detection is unchanged.**
- The root `id_versioned` is still the only deciding marker, and `_fid` is still cited as evidence.
- Only the Javadoc changed:
  - `LayoutDetector` now rests on the root versioned id being present in every legacy table,
    whether or not extensions were stored, and on the other markers adding misclassification risk
    without adding cases.
  - `LayoutDetection.isMixed` uses a nested `_fid` as its example rather than a terminology result.
- `LayoutDetectorTest` changed one Javadoc comment. `FileSourceDetectionTest` is unchanged.

**Docs.**
- `contracts/engine-semantics.md` documents the six-field System.Coding under *Coding and
  terminology*, and adds a row to the result-types table.
- `contracts/library-api.md` lists the change from seven fields under *Behaviour changes*.
- Decision 79 notes that its tolerance is superseded. Decision 100 notes that its reason is
  superseded and its rule stands.

**Conformance baselines.**
- The two SpEL-scoped, previous-layout-only rules in `fhirpath-js/config.yaml` are removed:
  `Unsupported resources` (#2418, StructureDefinition) and `Recursive ValueSet.expansion.contains
  not encoded`.
- No entries were added, and no run reported "Excluded test passed".
- `fhirpath-ptl/config.yaml` had no such rule.

### Approval gate

One existing test's expected value changed with the `_fid` removal, and it is not a struct type or
schema. It is left at its old expectation and `@Disabled`, with the gate named in the reason,
awaiting the owner:

- **`TerminologyResultRoundTripTest.dataCarryingTerminologyResultsIsReadBack`** (`library-api`).
  Its precondition is on the detector's output, the list of previous-layout markers.
  - Old expected value: `LayoutDetector.detect(patients.schema()).getPreviousLayoutMarkers()`
    contains `"translated._fid"`.
  - New actual value: the list is empty, because a `translate()` result no longer carries `_fid`.
  - With the precondition restated as `isEmpty()`, the round trip passed through every sink, in
    the full build below (`final-core.log`).
  - The test was written for decision 100's case, Pathling's own output carrying a marker. That
    output no longer exists, so the owner should decide whether to restate the precondition, as
    above, or retire the test.
  - **Owner, 2026-10-15: retired.** The case it was written for no longer exists, so the test class,
    which held only this test, is deleted.

Changes made under the struct-type exception, listed so the owner can check them:

- **Python `test_udfs.py`.** `CODING_TYPE` and `CodingRow` drop the `_fid` field. The expected rows
  are otherwise the same.
- **R `helper_default.R`.** `coding_row` drops `_fid`.

### Restated by the removal of the previous-layout arms, for review

These changes come from dropping the previous layout, not from `_fid`.

- **`QuantityCollectionTest`.** `fieldOfADecodedQuantityIsNormalisedOnce` is now
  `fieldOfADecodedQuantityIsReadFromTheStoredQuantity`.
  - It asserted one `CASE WHEN` over the previous layout and none over PoF.
  - It now asserts none.
- **`JoinKeyFunctionsDslTest.newLayoutStoresNoVersionedKey`.** Its control was that the search
  finds `id_versioned` in previous-layout data. It is now a hand-built schema carrying
  `id_versioned`.
- **`QuantitySearchPrecisionTest`.** The previous-layout rows go, including the expectation that
  the previous layout finds nothing for `eq0.2 mg`.
- **`AnsiTypeHintingTest`.** It keeps the PoF instant value.
- **`ResolveOrNullTest`.** The previous-layout cases go: the legacy extension map, decimal and
  quantity normalisation, and the versioned keys. The new-layout and pruned cases stay.

### Code review

Two reviewers, on standards and on the spec, reviewed the changes since `issue/2829`.

**Applied:**
- Two comments still described the legacy lookup: in `Collection.traverseExtension` and in
  `fhirpath/pom.xml`.
- `testVersionedKeysOverTheActiveLayout` is renamed `testVersionedKeys`.
- `YamlSupport.asStoredOnNewLayout` is renamed `asStored`.

**Left open, as judgement calls:**
- About ten tests repeat the "`PofDatasets.fromJson`, then write and read back with
  `PrunedSchemaReader`" helper. It could be extracted into `PofDatasets`.
- `DimensionProperty` now serves only `TestSchemaMode`, and the `test.layout` package name is now
  a slight misnomer.
- `ObjectDataSource` takes `FhirEncoders` only for its `FhirContext`. Its 58 callers were left
  alone.
- `ResolveOrNullTest`'s generic tolerant-reference fixtures still name columns `_fid` and
  `_extension`, and its datasets are still called `newLayout`, `newDecimals` and so on.
- Several pre-existing comments say "the new layout".

### Test runs on the final commit

All logs are in `.local/work/issue/2829-remove-legacy-normalisation/`.

- **The core build**, `mvn clean install -pl library-runtime -am`, with the SQL-on-FHIR submodule
  initialised. Log: `final-core.log`. Everything passed:

  | Module | Run | Failed | Skipped |
  | --- | --- | --- | --- |
  | `utilities` | 35 | 0 | 0 |
  | `fhir-schema` | 39 | 0 | 0 |
  | `encoders` | 601 | 0 | 0 |
  | `io` | 480 | 0 | 1 |
  | `terminology` | 538 | 0 | 0 |
  | `fhirpath` | 7366 | 0 | 922 |
  | `library-api` | 318 | 0 | 0 |

  That run had `TerminologyResultRoundTripTest` with its precondition restated. It was then
  disabled under the approval gate, a test-only change, and on its own it ran as 1 skipped
  (`gate-disabled.log`). The owner then retired it, and the class was deleted.

- **`legacy-export`**, `mvn clean install -pl legacy-export`: 38 run, 0 failed. Log:
  `final-legacy-export.log`.
- **Python:** 570 passed. Log: `final-pytest.log`.
  - Run with an empty Ivy home of its own, `ivy-py2`, through `PYSPARK_SUBMIT_ARGS`
    (`spark.jars.ivy`).
  - The `spark-submit-test` smoke step that Maven runs after pytest does not take that setting,
    so it used the shared `~/.ivy2.5.2`, which holds an older jar. Only that step is affected.
- **R:** 209 expectations passed, 0 failed. Run with an empty Ivy home of its own, `ivy-r3`, through
  `SPARK_CONF_DIR` (`r-conf3`). Log: `final-r-2.log`.
  - An earlier run, `final-r.log` with `ivy-r2`, also passed all 209.
  - That run's Maven build then failed in `maven-source-plugin` ("A zip file cannot include
    itself"), because `install` was run again without `clean`. The rerun with `clean` succeeded.
- **Jar check.** The retrieved `library-runtime` jar has SHA-1 `d9414a2abc` in `ivy-py2`, `ivy-r2`
  and `ivy-r3`, the same as in `library-runtime/target` and `~/.m2`.
  - Its `CodingSchema.class` contains no `_fid`.
  - It holds none of the removed normalisation classes.
