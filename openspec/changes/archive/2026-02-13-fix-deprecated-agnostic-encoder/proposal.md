## Why

The `FhirAgnosticEncoder` in `EncoderBuilder.scala` extends
`AgnosticExpressionPathEncoder`, which is deprecated in Spark 4.0 with a
message stating it will be removed in 4.1. While research shows the trait
survives in Spark 4.1.0, it will eventually be removed and we need a
replacement that uses supported APIs.

## What changes

- Replace `FhirAgnosticEncoder` (which extends `AgnosticExpressionPathEncoder`)
  with a plain `AgnosticEncoder` subclass that provides metadata only (data
  type, class tag). Construct `ExpressionEncoder` directly using its public case
  class constructor, passing our pre-built serialiser/deserialiser Expression
  trees.
- Remove the `@annotation.nowarn("cat=deprecation")` suppression.
- Remove the import and usage of `AgnosticExpressionPathEncoder`.

## Capabilities

### New capabilities

_None._

### Modified capabilities

_None — this is a purely internal refactor with no changes to externally
observable behaviour or requirements._

## Impact

- **Code**: `encoders/src/main/scala/au/csiro/pathling/encoders/EncoderBuilder.scala`
  only. No changes to `SerializerBuilder` or `DeserializerBuilder`.
- **Dependencies**: No new dependencies; continues to use the existing Spark
  catalyst encoder APIs (`AgnosticEncoder`, `ExpressionEncoder`).
- **Performance**: No impact. The same Catalyst Expression trees are used; only
  the mechanism for passing them to `ExpressionEncoder` changes.
- **Testing**: All existing encoder tests must continue to pass.
