# 02 — Port the engine tests that encode via FhirEncoders to the PoF layout

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-046, SC-002.

**What to build:** Several engine tests still build their input with the legacy `FhirEncoders`, so they exercise the legacy-layout branches that ticket 05 deletes. Port them to build their data in the PoF layout through the public read and encode path, keeping each test's intent:

- `AbsentElementTest`
- `ProjectedColumnTypeTest`
- `SiblingCombinationTest`
- `DivergentSchemaViewTest`
- `YamlTestRunnerTest.testJsonModel`

**Blocked by:** None — can start immediately

**Status:** done

- [x] No test in the list encodes through `FhirEncoders` or reads legacy-layout data.
- [x] Each ported test asserts what it asserted before. Where an assertion depended on the legacy shape, such as a fixed-scale decimal or `_fid`, it is restated against the PoF shape, and the change is called out for review.
- [x] A search confirms that no other test under `fhirpath` encodes via `FhirEncoders`, or lists those that do as further work on this ticket.
- [x] The `fhirpath` suite passes.

## Comments

### Implementation (branch `issue/2367-port-encoder-tests`)

The five tests now build their input with `PofDatasets`, a new test helper that reads FHIR JSON
through `FhirReader` and takes a `FhirContext` rather than `FhirEncoders`. They no longer follow the
`pathling.testLayout` dimension, and give the same results with `-Dpathling.testLayout=previous`.
`LayoutDatasets`' PoF branch delegates to `PofDatasets`, so ticket 05 can make it the only path.

The described schema that `PrunedSchemaReader` checked a removed path against was
`fhirEncoders.of(X).schema()`. It can now be the resource's definition instead, through
`PrunedSchemaReader.write(dataset, path, fhirContext, resourceType)`.
- A choice variant such as `component.valueString` resolves through its parent.
- An unqualified choice name such as `value` is refused, because it is never a stored column.
- `PrunedSchemaReaderTest` covers the new overload.

**Present-but-null controls (new coverage on PoF).** In `AbsentElementTest`,
`ProjectedColumnTypeTest` and `SiblingCombinationTest`, the "over the full schema" control used to
be a real control only in the previous-layout CI job. On PoF the fitted schema already lacked every
unpopulated element, so the control read the same schema as the test. The schema is now fitted
together with filler resources, through `PofDatasets.fromResources(..., fillers)`.
- The fillers populate every element a case removes, and they are left out of what is written.
- So on PoF the control reads each removed element as a present column holding null, and the test
  reads it absent.
- This state is a real one: it arises whenever one resource populates an element and another does
  not. It was previously reached only through the legacy normalisation that ticket 05 deletes.
- The fillers cover every pruned path. With `readWithout` temporarily made strict, every removal
  found its path in the written schema.
- All the controls pass unchanged.

### Restated assertions, for review

- **`AbsentElementTest.prunedSchemaLacksTheElement`.** Before: the encoder's dense schema carries
  the element. Now: the definitions describe it. The written schema now carries it on every
  layout, because a filler populates it; before, it carried it only on the previous layout.
- **`AbsentElementTest`, the decision 72 pair**, renamed from
  `repeatingPrimitiveMetadataOnlyIsKnownDivergenceFromReleasedEncoder` and
  `singularPrimitiveMetadataOnlyAgreesWithReleasedEncoder` to
  `nameHoldingOnlyRepeatingPrimitiveMetadataIsKept` and
  `nameHoldingOnlySingularPrimitiveMetadataIsKept`.
  - The released-encoder arm is gone: it asserted that the encoder answers `name.count()` = 0 for
    a name that holds only `_given`, and 1 for one that holds only `_family`. Those are facts about
    legacy-layout data, which the engine no longer reads (FR-046). The divergence stays recorded in
    the comments and the assertion messages.
  - The layout arm used to read a hand-written Spark schema (`{"given": null}` and
    `{"family": null}`), not the real input. It now reads the `_given` and `_family` JSON through
    `FhirReader`.
  - The layout does not yet store primitive metadata, so the reader drops that content and logs a
    warning. The result is exactly the hand-built shape, which the tests now assert: `name` holds
    only `given` as a null `array<string>`, or only `family` as a null `string`.
  - The answers are unchanged: `name.count()` = 1, `name.given.count()` = 0 and
    `name.family.count()` = 0.
  - Once primitive metadata is stored, these tests will need revisiting.
- **`DivergentSchemaViewTest.unnestingReturnsRowsFromBothFiles`** was pinned to the previous layout,
  because it needs the leaf carried as a null in the second file.
  - Now both batches are read together, under one fitted schema, and each batch's rows are written
    to its own file.
  - A new precondition asserts that both files carry `name.family`. The view assertions are
    unchanged.
- **`DivergentSchemaViewTest.unnestingReturnsRowsFromBothFilesWhenTheLeafIsAbsentFromOneFile`**
  no longer writes the second batch through a staging round trip with `readWithout("name.family")`.
  Read on its own, the batch's fitted schema already lacks the leaf, so the removal was a no-op.
  The existing assertion that exactly one file carries the leaf still checks this.
- **`YamlTestRunnerTest.testJsonModel`** reads the patient JSON directly. `testSimpleYaml` still uses
  `TestLayout.active()` through `YamlSupport`. That use is not an encoder use, and ticket 05 removes
  it.

No assertion depended on a fixed-scale decimal or on `_fid`.

### Search: other `fhirpath` tests that encode through `FhirEncoders`

All paths are relative to `fhirpath/src/test/java/au/csiro/pathling/`. Each file was checked at its
call sites, not by grep alone.

**Ported on this ticket.** These used `encoders.of(X).schema()` as the schema to check pruned paths
against, on every run. They now read their fixtures in the PoF layout and check paths against the
definitions:

- `search/PrunedPeriodSearchTest`
- `search/PrunedReferenceSearchTest`
- `search/PrunedTokenSearchTest`

**Handed to ticket 05.** These encode a `TestLayout.PREVIOUS` arm on every run, beside the PoF arm.
They are "test arms that run both layouts", which ticket 05 removes, so porting them here would only
be undone there. Ticket 05 lists them.

**No change needed.** These encode legacy data only when `-Dpathling.testLayout=previous` is set,
through `ObjectDataSource`, `LayoutDatasets` or the YAML resolver factories. They lose that path when
ticket 05 removes the dimension:

- `SearchColumnBuilderTest`
- `FhirSearchExecutorTest`
- `AnsiModeIndependenceTest`
- `TraceFunctionTest`
- `SingleResourceFhirPathTest`
- `SingleInstanceEvaluatorIntegrationTest`
- `DatasetEvaluatorTest`
- `AnsiTypeHintingTest`, whose expected value branches on `isPof()`
- `FhirViewTest`
- `FhirViewIntegrationTest`
- `ReferenceKeyJoinTest`
- the YAML/DSL suites, through `YamlTestBase`, `YamlCachedTestBase`, `RuntimeContext` and the
  resolver factories

**No encoding.** These build a `FhirEncoders` bean, or use the encoders only for its `FhirContext`.
The bean's nesting bound (`withMaxNestingLevel`) matters only on the previous layout:

- `UnitTestDependencies`
- `RepeatFunctionDslTest`
- `RepeatAllFunctionDslTest`
- `YamlReferenceImplTest`
- `AnnotationFreeSuiteTest`, which pins `TestLayout.POF`
