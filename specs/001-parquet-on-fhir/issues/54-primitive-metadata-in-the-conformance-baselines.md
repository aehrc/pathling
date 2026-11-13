# 54 — Primitive metadata in the conformance baselines and the default build

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-034, US6. Raised from ticket 52 (Primitive metadata in the engine; owner, 2026-11-13). Decision 141 (D14).

**What to build:** Ticket 52 reads a primitive's `id` and `extension` behind `FhirpathConfiguration.readPrimitiveMetadata`, off by default, so the conformance suites and the rest of the build exercise it only through `PrimitiveMetadataTest`.

- **(a) Baselines that depend on the configuration.** The fhirpath-js and fhirpath-ptl exclusion baselines (`fhirpath/src/test/resources/fhirpath-js/config.yaml`, `fhirpath-ptl/config.yaml`) hold one set of exclusions, whatever the engine's configuration. Make them depend on it, for example through exclusions that apply only to a given configuration, so that one case can be excluded with the switch off and expected to pass with it on.
- **(b) Re-enable the fhirpath.js cases about primitive metadata.** With the switch on and the YAML harness storing primitive metadata (`PofDatasets.fromJson` reading with `SchemaConfiguration.storePrimitiveMetadata`), these 18 excluded cases pass (ticket 52's local run, 2026-11-13):
  - #437 "'is' operator with extensions on primitive types not supported" (`testTypes` 19, 20);
  - #437 "as() operator with count() on primitive elements with extensions but no value" (`testTypes` 38, 46);
  - #437 "Union/combine with count() on primitive elements with extensions but no value" (`testCombining` 8, 9, 14);
  - "Support for null values within FHIR elements" (`testExistence` 2, 14);
  - #437 "isDistinct() on primitive elements with extensions but no value" (`testExistence` 68);
  - #437 "extensions on primitive types not supported" (`testExtension` 3, 7, 8) and "ID on primitive types not supported" (`testExtension` 17);
  - #437 "where() with count() filtering on id attribute of primitive elements not supported" and "repeat() on id attribute of primitive elements not supported" (`testFilteringAndProjection` 6, 21);
  - #437 "Extensions on primitive types not supported" (`testFhirR4` 621);
  - #2580 "Primitive .id extension not accessible after trace" (`testUtilityFunctions` 2).
- **(c) Run the engine with primitive metadata on by default in the build**, the switch on and the test data stored with the metadata group.

**Context from ticket 52's run of the engine suite with the switch forced on:**

- Search reads an evaluated primitive as a bare value: `SearchColumnBuilder.matchedColumn` converts a decimal only, so with the switch on every string, token, URI and number search fails at analysis (`FhirSearchExecutorTest`, `SearchColumnBuilderTest`, `SearchValueSystemDecimalTest`, `SelfJoinAmbiguityTest`, `JoinConditionTest`). Search has no switch today; it needs `withoutPrimitiveMetadata()` there before it gets one.
- Tests that read the engine's raw form through `CollectionDataset.toIdValueDataset()` or assert a plan's shape expect a bare value where the switch gives a structure: `TraceFunctionTest`, `DatasetEvaluatorTest`, `SingleResourceFhirPathTest`, `ShapeReconciliationTest`, `ComparisonKeyTest`, `DateRangeComparisonTest`, `StoredQuantityTest`, `QuantityExtensionThroughProjectionTest`, `IndexedQuantityExtensionTest`, `AnnotationsByExampleTest` and `EmissionShapesTest`. Running the build with the switch on needs each of them to read the output form or to state its configuration.
- Python, R and the server do not expose the switch yet. Server exposure belongs here (owner,
  2026-11-13): the switch is deliberately not on `QueryConfiguration`, which the server binds from
  its Spring configuration, so exposing it there is a decision of this ticket.

**Blocked by:** 52.

**Status:** needs-triage

- [ ] The exclusion baselines can differ by engine configuration, and the build runs both suites with the switch on and off.
- [ ] The cases listed in (b) run, and pass, with the switch on.
- [ ] The build runs the engine with primitive metadata on by default, and the tests above read the output form or state their configuration.
- [ ] Whether and how the server exposes the switch is decided, and built if it is to be.
