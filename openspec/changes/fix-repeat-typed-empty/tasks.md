## 1. Baseline and WIP review

- [x] 1.1 Run `tests/repeat.json` via `FhirViewShareableComplianceTest` on a
      clean working tree (revert in-flight `Expressions.scala`,
      `RepeatSelection.java` edits) to confirm the three failing tests
      reproduce against `main`-equivalent code.
- [x] 1.2 Run `deep_nesting.json` on a clean working tree to confirm tests
      1 and 2 fail with `ClassCastException` (and 3, 4 fail with the
      analogous `forEach` cliff — these are out of scope but record the
      baseline).
- [x] 1.3 Audit the working-tree WIP in `Expressions.scala` against the
      design decisions. Identify which edits become redundant once the
      typed-empty fallback is in place and which (if any) should be
      retained as independent cleanups (`t(node)→t(newValue)`, formatting
      changes).

## 2. ProjectionSchema helper

- [x] 2.1 Add `au.csiro.pathling.projection.ProjectionSchema` with a
      static `structTypeOf(ProjectionResult)` returning Spark `StructType`.
- [x] 2.2 Implement per-`ProjectedColumn` field derivation:
      `sqlType` > FHIR `type` > Collection FHIR type > Materializable
      column expr `dataType()`.
- [x] 2.3 Implement the static FHIR primitive → Spark type map covering
      `id`, `string`, `uri`, `url`, `code`, `oid`, `uuid`, `markdown`,
      `canonical`, `base64Binary`, `integer`, `positiveInt`, `unsignedInt`,
      `decimal`, `boolean`, `date`, `dateTime`, `instant`, `time`.
      Throw `UnsupportedOperationException` for any other FHIR type.
- [x] 2.4 Wrap field types in `ArrayType` when
      `RequestedColumn.collection()` is true.
- [x] 2.5 Unit-test `ProjectionSchema.structTypeOf` covering: explicit
      `sqlType`, explicit FHIR `type`, inference via Materializable,
      `collection=true` wrapping, nested clause shapes (grouping +
      forEach), and the unsupported-type throw.

## 3. Encoder plumbing

- [x] 3.1 Add `expectedElementType: Option[StructType]` field to
      `UnresolvedTransformTree` (Scala). Default to `None` in existing
      constructors.
- [x] 3.2 In `UnresolvedTransformTree.mapChildren`, when FIELD_NOT_FOUND
      fires at the root (`parentType.isEmpty`) and `expectedElementType`
      is `Some(t)`, return `Cast(CreateArray(Seq.empty), ArrayType(t))`.
      Otherwise fall back to today's `CreateArray(Seq.empty)`.
- [x] 3.3 Confirm inner-node FIELD_NOT_FOUND (`parentType.nonEmpty`)
      continues to return `CreateArray(Seq.empty)`.
- [x] 3.4 Add a `ValueFunctions.transformTree(...)` overload accepting
      `StructType expectedElementType`; delegate to a single shared
      constructor of `UnresolvedTransformTree`. Existing overloads pass
      `Option.empty()`.
- [x] 3.5 Update `UnresolvedExpressionsTest` to cover the new field and
      its `withNewChildrenInternal` propagation.

## 4. RepeatSelection wiring

- [x] 4.1 In `RepeatSelection.evaluate`, after computing `schemaResult =
      component.evaluate(schemaContext)`, derive `expectedElement =
      ProjectionSchema.structTypeOf(schemaResult)`.
- [x] 4.2 Pass `expectedElement` into each `ValueFunctions.transformTree`
      invocation via the new overload.
- [x] 4.3 Revert in-flight working-tree edits in `RepeatSelection.java`
      that commented out `ValueFunctions.emptyArrayIfMissingField(...)`
      unless audit (1.3) determines they are still load-bearing.

## 5. Test enablement and verification

- [x] 5.1 Remove the `repeat - repeat inside repeat`,
      `repeat - repeat with forEach with repeat (triple nesting)`, and
      `repeat - repeat inside repeat inside repeat` exclusions from
      `FhirViewShareableComplianceTest` if any are present (they are not
      currently excluded — verify before removing nothing).
- [x] 5.2 Run `FhirViewShareableComplianceTest` and confirm all 19
      `tests/repeat.json` cases pass.
- [x] 5.3 Run `deep_nesting.json` tests 1 and 2 and confirm they pass.
      Tests 3 and 4 (forEach cliff) remain failing — out of scope.
- [x] 5.4 Run the full `fhirpath` test suite (`mvn -pl fhirpath test`)
      and confirm no regressions outside the listed scope.
- [x] 5.5 Run the full `encoders` test suite (`mvn -pl encoders test`)
      to verify `ExpressionsCodegenTest` and other transformTree callers
      remain green.

## 6. Cleanup and documentation

- [x] 6.1 Reconcile working-tree edits committed elsewhere on the branch
      with the final implementation; ensure no dead WIP remains.
- [ ] 6.2 Update commit history on branch `2619` so the change set
      consists of focused, individually-reviewable commits (new helper,
      encoder plumbing, RepeatSelection wiring, test exclusions).
- [ ] 6.3 File a follow-up issue for the symmetric `forEach` typed-empty
      fallback (deep_nesting tests 3 and 4) referencing this change for
      context.
- [ ] 6.4 Update `repeat-directive` capability spec (post-merge) when
      this change is archived, per the OpenSpec archive flow.
