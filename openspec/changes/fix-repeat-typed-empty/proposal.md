## Why

The expanded SQL on FHIR v2 `repeat` test suite (PR FHIR/sql-on-fhir-v2#348)
exposed three crashes in Pathling's `repeat` directive when the recursive
traversal extends past the encoder's `maxNestingLevel`. The failing tests
(`repeat inside repeat`, `repeat with forEach with repeat`, `repeat inside
repeat inside repeat`) all crash with `ClassCastException: NullType cannot be
cast to StructType` inside `StructProduct`, because the FIELD_NOT_FOUND
fallback in `UnresolvedTransformTree` emits an untyped `Array<NullType>` that
sibling column combination cannot type-check against. Issue #2619 tracks
closing this conformance gap.

## What Changes

- Derive the expected element `StructType` for a `repeat` directive's output
  from its projection clause's declared columns (names, FHIR/SQL types,
  `collection` flags).
- Plumb that `StructType` through `ValueFunctions.transformTree` into
  `UnresolvedTransformTree`, and use it in the root FIELD_NOT_FOUND fallback
  to emit `Cast(empty, ArrayType(struct))` instead of an untyped empty array.
- Introduce a new `ProjectionSchema` helper class in
  `au.csiro.pathling.projection` that converts a `ProjectionResult` into a
  Spark `StructType`, backed by a static FHIR-primitive → Spark-type map.
- Keep the existing untyped-fallback path as the default when no expected
  element type is supplied, so non-`repeat` callers of `transformTree` are
  unaffected.

## Capabilities

### New Capabilities

- `repeat-directive`: SoF v2 `repeat` execution rules, including
  the typed-empty contract when recursive traversal exits the encoder's
  declared schema.

### Modified Capabilities

<!-- None — this introduces a new capability spec. -->

## Impact

- `fhirpath/src/main/java/au/csiro/pathling/projection/RepeatSelection.java` —
  derives `StructType`, passes to `transformTree`.
- `fhirpath/src/main/java/au/csiro/pathling/projection/ProjectionSchema.java` —
  **new** helper class.
- `encoders/src/main/java/au/csiro/pathling/encoders/ValueFunctions.java` —
  new overload accepting `StructType expectedElementType`; existing overloads
  delegate with `Optional.empty()` to preserve current behaviour.
- `encoders/src/main/scala/au/csiro/pathling/encoders/Expressions.scala` —
  `UnresolvedTransformTree` gains an `expectedElementType:
  Option[StructType]` field used in the root FIELD_NOT_FOUND branch.
- `fhirpath/src/test/resources/viewTests/deep_nesting.json` — regression tests
  1 and 2 (repeat-at-cap) must pass after the change. Tests 3 and 4
  (forEach-past-cap) remain failing and are tracked separately.
- `fhirpath/src/test/java/au/csiro/pathling/views/FhirViewShareableComplianceTest.java` —
  `repeat` exclusions removed once tests 88, 94, 96 pass.

Out of scope for this change:

- `%rowIndex` support (separate issue; row_index test exclusions remain).
- Same typed-empty treatment for `forEach` (deep_nesting tests 3 and 4
  remain failing — separate follow-up).
- Refactoring the static FHIR → Spark type map into a
  `Materializable.getMaterialisedType()` method on each Collection class.
