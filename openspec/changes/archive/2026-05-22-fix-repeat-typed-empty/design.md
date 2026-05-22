## Context

The expanded SoF v2 `repeat` test suite (FHIR/sql-on-fhir-v2#348) exposes
three crashes in Pathling's `repeat` directive when a recursive traversal
extends past the encoder's `maxNestingLevel`:

- `repeat - repeat inside repeat`
- `repeat - repeat with forEach with repeat (triple nesting)`
- `repeat - repeat inside repeat inside repeat`

All three fail with the same `ClassCastException: NullType cannot be cast to
StructType` thrown by `StructProduct.dataType`.

### Failure mode

Pathling's FHIRPath evaluator is schema-agnostic: it uses HAPI definitions,
not the Catalyst-encoded struct schema. For a recursive element such as
`Item.item`, `Collection.traverse("item")` always succeeds at evaluator level,
even when the encoded struct at the current nesting depth has no `item`
field.

When `RepeatSelection.evaluate` then invokes
`ValueFunctions.transformTree(value, extractor, [ctx -> ctx.item], ...)` at
that depth, the Catalyst resolver inside `UnresolvedTransformTree.mapChildren`
raises `AnalysisException(FIELD_NOT_FOUND)`. The current handler returns
`CreateArray(Seq.empty)` — an `Array<NullType>`. When that untyped empty
array flows up into a surrounding `StructProduct(Array<Struct<...>>,
Array<NullType>)`, `StructProduct.dataType` attempts
`.asInstanceOf[StructType]` on the `NullType` element type and crashes.

### Why intermediate fixes don't work

These alternatives were prototyped and ruled out before this change:

- `safeExtractor(Cast(null, ArrayType(StructType(Seq()))))` — invokes the
  extractor on a fake empty-struct input. Whenever the extractor contains a
  nested `repeat`, the nested `UnresolvedTransformTree` re-encounters
  FIELD_NOT_FOUND against the empty struct and the cascade still ends in
  `Array<NullType>`.
- `Cast(CreateArray(Seq.empty), ArrayType(StructType(Seq())))` — returns a
  typed `Array<Struct<>>`. Avoids the ClassCast, but the empty struct
  contributes zero fields. `Projection.execute` then derives column names
  from the resultColumn's Catalyst schema and silently drops the projection
  clause's output column.
- Inferring a fake type from any resolved sub-expression of the failed node
  — works for the pure `repeat`-inside-`repeat` cases but fails when the
  inner repeat lives inside a `forEach` whose lambda variable is a different
  type (e.g. `Answer`) that lacks the fields the repeat's extractor needs.
- Reading `schemaResult.getResultColumn().expr().dataType()` — appealing
  because the schema result is already computed, but in deep cases the
  schema column itself contains the same unresolvable traversal, so
  `.dataType` is unreliable.

The common failure of every alternative is sourcing the type from somewhere
that itself depends on Catalyst resolution of the failed traversal. This
change sources the type from declared projection metadata instead.

## Goals / Non-Goals

**Goals:**

- All 19 tests in `tests/repeat.json` pass against the `FhirViewShareable`
  compliance suite, including the three currently failing nested cases.
- The two `repeat`-at-cap regressions in `deep_nesting.json` pass.
- The encoder layer (`ValueFunctions`, `UnresolvedTransformTree`) stays
  ignorant of FHIR concepts — only Spark types cross the module boundary.
- Existing non-SoF callers of `transformTree` keep their current behaviour
  via an unchanged overload.

**Non-Goals:**

- `%rowIndex` support (separate issue; row_index test exclusions stay).
- Typed-empty treatment for `forEach` (`deep_nesting.json` tests 3 and 4
  remain failing — separate follow-up).
- Refactoring the FHIR → Spark primitive type map into a method on each
  `Materializable` Collection class.
- Adding support for non-primitive column outputs (Coding, Reference,
  Quantity, etc.) — same constraint as today.
- Changes to `pom.xml` SOF compliance profile gating — orthogonal CI
  decision.

## Decisions

### D1. Schema derivation added directly to `ProjectedColumn` and `ProjectionResult`

*As built:* `ProjectedColumn.getSqlType()` and `ProjectionResult.getSqlType()`
carry schema derivation inline on the existing data-plane records.
`RepeatSelection` calls `schemaResult.getSqlType()` directly. A separate
`ProjectionSchema` helper class was prototyped but deleted because the two
small methods did not justify a new class.

**Original plan:** A separate `au.csiro.pathling.projection.ProjectionSchema`
class with `structTypeOf(ProjectionResult)`. Rejected during implementation
because the surface area turned out to be two short methods; the
single-responsibility concern that motivated the separate class did not
outweigh the overhead of an extra file with no independent users.

### D2. FHIR-primitive → Spark-type lookup routes through `FhirPathType.forFhirType`

*As built:* `ProjectedColumn.getSqlType()` calls
`FhirPathType.forFhirType(fhirType).map(FhirPathType::getSqlDataType)` rather
than a hand-rolled `switch`. This routes through the canonical mapping already
maintained in `FhirPathType` and avoids duplicating the table.

**Original plan:** A static `switch` inside `ProjectionSchema`. Replaced during
implementation when it became clear that `FhirPathType.FHIR_TYPE_TO_FHIR_PATH_TYPE`
already encodes the correct mappings and the old `sparkTypeFor()` switch had three
bugs (`BASE64BINARY→BinaryType`, `DECIMAL→StringType`, `INSTANT→TimestampType`)
that delegating to `FhirPathType` silently fixes.

**Alternative considered:** Adding `DataType getMaterialisedType()` to the
`Materializable` interface so each Collection class declares its own
projected Spark type. Rejected for this change because it touches every
primitive Collection and the centralized lookup is sufficient for the
declared types `ProjectedColumn` sees. Refactoring into a Collection-local
method is a clean follow-up if Pathling later extends complex-type support.

### D3. Spark-typed boundary between fhirpath and encoders

The new parameter passed to `transformTree` is a Spark `StructType`, not a
FHIR-aware structure (e.g. `List<ProjectedColumn>`). The encoders module
sits below fhirpath in the dependency hierarchy and must not depend on FHIR
concepts.

**Alternative considered:** Passing FHIR metadata. Rejected because it
would require the encoder module to depend on fhirpath — a dependency
inversion.

### D4. `expectedElementType` is optional on `transformTree`

The encoder-facing API gains an additional overload accepting
`StructType expectedElementType`. The existing overloads remain unchanged
and pass `Option.empty()` through to `UnresolvedTransformTree`. When
`expectedElementType` is absent, the FIELD_NOT_FOUND fallback continues
to return the untyped `CreateArray(Seq.empty)` it always has.

**Rationale:** Non-SoF callers of `transformTree` (e.g. existing tests in
`ExpressionsCodegenTest`) keep their current behaviour. Only
`RepeatSelection.evaluate` opts into the typed fallback.

### D5. Inner FIELD_NOT_FOUND fallback unchanged

`UnresolvedTransformTree.mapChildren` distinguishes the root case
(`parentType.isEmpty`) from the inner case (`parentType.nonEmpty`). Only
the root case uses `expectedElementType`. Inner cases continue to return
`CreateArray(Seq.empty)` because the surrounding `Concat` correctly
upcasts `Array<NullType>` against the typed sibling arrays.

**Rationale:** The ClassCast only manifests at the root of a repeat (where
the empty result feeds StructProduct directly). Inner empties never reach
StructProduct without going through a typed `Concat` first.

### D6. Schema-context selection unchanged

`RepeatSelection.evaluate` keeps its current schema-context selection —
prefer the first non-empty starting node, fall back to `withEmptyInput()`.
The schema derivation runs on the `ProjectionResult.results` list (declared
RequestedColumn names + types), which is independent of whether the
schema-context column itself contains unresolvable traversals.

**Rationale:** `ProjectionSchema.structTypeOf` consults `RequestedColumn`
metadata first (sqlType, then type, then collection FHIR type); it falls
back to `Materializable.getExternalValue(...).expr().dataType()` only when
none of the declared annotations is present. The failing tests all carry
explicit `type` annotations, so the fallback path is not exercised by
this change.

## Risks / Trade-offs

[Risk: Stale `ExpressionsCodegenTest` callers may rely on the untyped empty
behaviour]
→ Mitigation: Keep the existing `transformTree` overload signatures
intact; new overload accepts the additional parameter. Verify the existing
codegen tests stay green.

[Risk: `RequestedColumn` may have neither `type` nor `sqlType` (path
inference)]
→ Mitigation: `ProjectionSchema` falls back to
`Materializable.getExternalValue(collection).expr().dataType()`. For a
column whose path doesn't traverse the broken cliff, this still resolves
cleanly. If a column's inferred type itself touches the unresolvable
traversal, the same failure mode could surface inside the helper — but
none of the SoF v2 tests in scope leave type undeclared.

[Risk: New helper duplicates Catalyst-schema knowledge that already lives
in encoder classes]
→ Mitigation: Static map is small (8 primitive types) and matches
documented FHIR R4 → Spark behaviour. Adding new primitive types is a
one-line change. Long-term consolidation into a single source is a
follow-up if more complex mappings appear.

[Risk: Three currently-failing tests pass but other repeat cases regress]
→ Mitigation: Re-run the entire `repeat.json` suite plus
`deep_nesting.json` plus the broader `FhirViewShareableComplianceTest` as
part of the change. The compliance run is reportable from the
`sofComplianceReport` profile.

[Risk: WIP changes in working tree (Expressions.scala `safeExtractor`,
`t(node)→t(newValue)`) become redundant once the typed fallback lands]
→ Mitigation: Review which WIP edits are still load-bearing before
committing. The typed fallback may make `safeExtractor` unnecessary; the
`t(newValue)` change is an independent cleanup and can be evaluated on its
own merit.

## Migration Plan

No data migration. The change is additive: a new `transformTree` overload
plus a new `ProjectionSchema` helper. Existing `repeat` views that did not
trip the cliff behave identically. Existing `forEach` views are unchanged
by this change.

Rollback: revert the change set. No persistent state involved.

## Open Questions

- Should the `pom.xml` SOF compliance profile change be reverted once the
  three failing tests pass, restoring SOF compliance as a default-build
  gate? (Tracked as a separate CI policy decision, not part of this
  change.)
- Are any of the working-tree WIP edits in `Expressions.scala`
  (`safeExtractor`, `t(node)→t(newValue)`) still required after the
  typed-fallback is in place? Decision: re-evaluate during implementation.
