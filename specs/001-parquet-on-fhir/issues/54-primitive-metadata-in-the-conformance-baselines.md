# 54 — Primitive metadata always on

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-034, US6. Raised from ticket 52 (Primitive metadata in the engine; owner, 2026-11-13). Decisions 141 (D14), amended by decision 154.

**What to build:** Ticket 52 reads a primitive's `id` and `extension` behind `FhirpathConfiguration.readPrimitiveMetadata`, off by default, so the conformance suites and the rest of the build exercise it only through `PrimitiveMetadataTest`, and Search fails with it on. Primitive metadata has one mode, on (owner, 2026-11-13, decision 154). This ticket removes the switch and makes the whole build pass in that mode.

- **Remove the switch everywhere.** `FhirpathConfiguration.readPrimitiveMetadata`; `PathlingContext.Builder.readPrimitiveMetadata` and `isReadPrimitiveMetadata`, a public API that ticket 52 added and that never shipped; the four-argument `FhirViewExecutor` constructor; and the tests that compare the switch on with the switch off. Every resource is resolved under the wrapped definitions.
- **A primitive leaves the engine as its bare value.** The structure `struct(value, id, extension, <annotation fields>)` is only the form a primitive takes while an expression is evaluated, so that `id` and `extension` can be navigated, and it is converted to the value by `toEngineForm()` wherever the engine hands a value on (as view output, comparison and function parameters already do). It leaves only where the expression itself ends in `id` or `extension`.
- **Search.** `SearchColumnBuilder.matchedColumn` converts a wrapped primitive, as it converts a decimal, and leaves a stored quantity as held, because `QuantityMatcher` reads its stored annotations (`toEngineForm()` would drop them). Search then needs no switch and no `withoutPrimitiveMetadata()` of its own.
- **Remove the 18 fhirpath.js exclusions** that primitive metadata resolves, in `fhirpath/src/test/resources/fhirpath-js/config.yaml` (and `fhirpath-ptl/config.yaml` where a case there passes too), and make the YAML harness store primitive metadata (`PofDatasets.fromJson` reading with `SchemaConfiguration.storePrimitiveMetadata`). The cases pass (ticket 52's local run, 2026-11-13):
  - #437 "'is' operator with extensions on primitive types not supported" (`testTypes` 19, 20);
  - #437 "as() operator with count() on primitive elements with extensions but no value" (`testTypes` 38, 46);
  - #437 "Union/combine with count() on primitive elements with extensions but no value" (`testCombining` 8, 9, 14);
  - "Support for null values within FHIR elements" (`testExistence` 2, 14);
  - #437 "isDistinct() on primitive elements with extensions but no value" (`testExistence` 68);
  - #437 "extensions on primitive types not supported" (`testExtension` 3, 7, 8) and "ID on primitive types not supported" (`testExtension` 17);
  - #437 "where() with count() filtering on id attribute of primitive elements not supported" and "repeat() on id attribute of primitive elements not supported" (`testFilteringAndProjection` 6, 21);
  - #437 "Extensions on primitive types not supported" (`testFhirR4` 621);
  - #2580 "Primitive .id extension not accessible after trace" (`testUtilityFunctions` 2).
- **The full build passes**: the engine suites, Search, Python and R, over data stored with the metadata group.
- **Dropped: baselines that depend on the configuration.** The ticket's first version asked for exclusions that apply to one configuration only; with one mode, the baselines hold one set of exclusions again.

**Tests that read the engine's raw form.** Ticket 52's run with the switch forced on showed these tests, which read the in-flight form through `CollectionDataset.toIdValueDataset()` or assert a plan's shape, expecting a bare value where the structure now is: `TraceFunctionTest`, `DatasetEvaluatorTest`, `SingleResourceFhirPathTest`, `ShapeReconciliationTest`, `ComparisonKeyTest`, `DateRangeComparisonTest`, `StoredQuantityTest`, `QuantityExtensionThroughProjectionTest`, `IndexedQuantityExtensionTest`, `AnnotationsByExampleTest` and `EmissionShapesTest`; and the Search tests that failed at analysis: `FhirSearchExecutorTest`, `SearchColumnBuilderTest`, `SearchValueSystemDecimalTest`, `SelfJoinAmbiguityTest` and `JoinConditionTest`. They are higher-level tests, whose expectations need the owner's approval. **The owner approved, on 2026-11-13, that they move from the raw form to the output form** (or, for a plan-shape test, assert the new shape). The implementing agent still reports every change to them.

**Out of scope:** server exposure (the switch is deliberately not on `QueryConfiguration`, and with one mode there is no switch to expose), and an optional off switch, which is ticket 55 (Optional switch to skip reading primitive metadata, in stage 5).

**Blocked by:** 52.

**Status:** ready-for-agent

- [x] No switch for primitive metadata remains in the code, including the `PathlingContext.Builder` option and the four-argument `FhirViewExecutor` constructor.
- [x] The cases listed above run, and pass, with their exclusions removed.
- [x] Search of every parameter type passes with primitive metadata on, and a quantity search still reads its stored annotations.
- [x] The tests above read the output form (or assert the new plan shape), and the full build passes, including Python and R.

## Comments

### Owner decision, 2026-11-13 (decision 154)

Primitive metadata has one mode, on; the off switch is removed from the code and becomes ticket 55 (in stage 5, to triage). A primitive leaves the engine as its bare value. Server exposure is out of scope. This replaces the ticket's first version, which kept the switch off by default and asked for the baselines to depend on it and for a full build run in both modes.

### Built, 2026-11-13 (branch issue/2832_54_primitive-metadata-always-on)

- **The switch is gone.** `FhirpathConfiguration.readPrimitiveMetadata`, `PathlingContext.Builder.readPrimitiveMetadata` and `isReadPrimitiveMetadata`, the four-argument `FhirViewExecutor` constructor and `DatasetEvaluatorBuilder.withConfiguration` (added only for it) are removed; `SingleResourceEvaluatorBuilder` always resolves under `PrimitiveMetadataResolver`. A definition that is not wrapped (an arbitrary object in the YAML harness) still gives a primitive no children.
- **Search.** `matchedColumn` converts a wrapped primitive, except a date, which is matched as held (its matcher reads only text and bounds), because converting it lost the filter pushed down on a stored bound (`AnnotationPlanBaselineTest.aDateSearchPushesAFilterOnTheStoredRange`). A stored quantity is left as held.
- **Pushdown.** With metadata read, a singular primitive's guard is a disjunction (`value`, `_x.id`, `_x.extension` present), which `AnnotationOrElse` did not see through, so `DatePushdownTest` and `AnnotationPlanBaselineTest` lost their pushed filters. The accessor now sees through a disjunction of presence tests of which one tests the value (`ResolveOrNoneTest.accessorSeesThroughADisjunctionThatTestsTheValue`).
- **Decimals.** Conversion dropped every null, so a repeating decimal that does not decode lost its null item (`AnnotatedTypesParityTest`, `(component.valueQuantity.value | 2).count()`). It now drops only elements without a value.
- **Fixtures.** `PofDatasets.fromJson` stores primitive metadata. `AbsentElementTest` covers a name holding only `_given` or `_family` both ways (owner, 2026-11-13): read without the metadata (default ingest), its expectations stand; stored with it, `name.given.count()` is 2 and `name.family.count()` is 1, as fhirpath.js counts them.
- **Baselines.** The 18 fhirpath.js exclusions are removed (seven rules emptied and deleted); fhirpath-ptl needed no change.
- **Sonar.** `java:S3358` fixed (`DefaultRepresentation.base64Text`); `java:S1845` fixed (`StoredPrimitive.value` renamed `valueOf`).
