## Context

The `FhirAgnosticEncoder` in `EncoderBuilder.scala` extends
`AgnosticExpressionPathEncoder[T]`, a trait deprecated in Spark 4.0 with a
stated removal target of 4.1. While research confirms it survives in Spark
4.1.0, it will eventually be removed and we should eliminate the dependency now.

The current flow:

1. `SerializerBuilder` builds a Catalyst Expression tree for serialisation.
2. `DeserializerBuilder` builds a Catalyst Expression tree for
   deserialisation.
3. `FhirAgnosticEncoder` wraps these in an `AgnosticExpressionPathEncoder`
   with `toCatalyst`/`fromCatalyst` methods.
4. `ExpressionEncoder.apply(agnosticEncoder)` calls Spark's
   `SerializerBuildHelper`/`DeserializerBuildHelper`, which pattern-match on
   `AgnosticExpressionPathEncoder` and delegate back to our
   `toCatalyst`/`fromCatalyst`.

This round-trip through `AgnosticExpressionPathEncoder` is unnecessary because
we already have the final expression trees.

## Goals / Non-goals

**Goals:**

- Remove usage of the deprecated `AgnosticExpressionPathEncoder` trait.
- Remove the `@annotation.nowarn("cat=deprecation")` suppression.
- Preserve identical runtime behaviour and performance.

**Non-goals:**

- Changing the serialiser/deserialiser expression-building logic.
- Migrating to `TransformingEncoder` + `Codec` (unnecessary given the simpler
  approach).
- Supporting Spark versions prior to 4.0.

## Decisions

### Direct `ExpressionEncoder` construction

**Decision**: Construct `ExpressionEncoder` directly using its case class
constructor, bypassing the `apply(AgnosticEncoder)` factory that triggers
the `SerializerBuildHelper`/`DeserializerBuildHelper` pattern-matching path.

**How**: The `ExpressionEncoder` case class constructor is public:

```scala
case class ExpressionEncoder[T](
    encoder: AgnosticEncoder[T],
    objSerializer: Expression,
    objDeserializer: Expression)
```

We provide a plain `AgnosticEncoder` (metadata only) alongside our pre-built
serialiser/deserialiser expressions:

```scala
private class FhirAgnosticEncoder[T](
    override val dataType: DataType,
    override val clsTag: ClassTag[T]
) extends AgnosticEncoder[T] {
  override def isPrimitive: Boolean = false
}

val agnosticEncoder = new FhirAgnosticEncoder[Any](
    schema, ClassTag(fhirClass))
new ExpressionEncoder(agnosticEncoder, serializerExpr, deserializerExpr)
```

**Alternatives considered**:

- `TransformingEncoder` + `Codec`: Requires rewriting
  serialisation/deserialisation as object-level conversions (FHIR resource to/
  from `InternalRow`). Major refactor with potential performance degradation
  from intermediate object materialisation. Rejected as disproportionate.
- Spark package trick (`package org.apache.spark.sql`): Unnecessary since the
  constructor is already public.
- Do nothing: Defers risk without justification given the simple fix available.

## Risks / Trade-offs

- **Internal API stability**: The `ExpressionEncoder` case class constructor
  could change in future Spark versions. Mitigation: case classes provide strong
  backward compatibility in Scala, and the entire encoder module already depends
  heavily on Spark internals (Catalyst expressions, `BoundReference`,
  `CreateNamedStruct`, etc.). This adds no incremental risk.
- **Semantic difference**: The `apply(AgnosticEncoder)` factory performs
  additional wrapping (e.g., `AssertNotNull` on `BoundReference` for top-level
  structs) that the direct constructor path does not. Our serialiser expression
  already includes null handling via `If(IsNull(...))`, so this is not a
  concern.
