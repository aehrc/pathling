/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.csiro.pathling.sql

import au.csiro.pathling.encoders.ColumnFunctions.{columnOrNull, resolveOrNull}
import au.csiro.pathling.encoders.UnresolvedTransformTree
import au.csiro.pathling.encoders.ValueFunctions.{transformTree, unnest, variantTransformTree}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.function.UnaryOperator

/**
 * Tests that the recursive tree traversal stops where a tolerant traversal step resolves to the
 * fallback of an absent element (T110a), rather than where a direct field reference throws.
 *
 * The traversals are built as the engine builds them after T110: every step is the tolerant
 * traversal expression, and the root is the tolerant table-column reference. Where the same shape
 * can also be traversed with direct field references, whose termination is the FIELD_NOT_FOUND
 * catch, the two are compared, so that the new signal is shown to give the same rows.
 */
class TransformTreeTerminationTest extends SparkSessionSupport {

  private val Bottom: DataType = ArrayType(NullType)

  private val LinkIdElement: StructType = new StructType().add("linkId", StringType)

  // Three levels of items, where the deepest level has no item field, as the encoder's nesting
  // bound leaves it.
  private def nested: DataFrame = parquet("nested",
    """select 1 as id, array(named_struct('linkId', '1', 'text', 'L0', 'item',
      |  array(named_struct('linkId', '2', 'text', 'L1', 'item',
      |    array(named_struct('linkId', '3', 'text', 'L2')))))) as items""".stripMargin)

  // A table that has no items column at all.
  private def rootless: DataFrame = parquet("rootless", "select 1 as id")

  private def op(f: Column => Column): UnaryOperator[Column] = (c: Column) => f(c)

  private def tolerantLinkId: UnaryOperator[Column] = op(resolveOrNull(_, "linkId", StringType))

  private def tolerantItem: UnaryOperator[Column] = op(c => unnest(resolveOrNull(c, "item", Bottom)))

  private def tolerantAnswerItem: UnaryOperator[Column] =
    op(c => unnest(resolveOrNull(unnest(resolveOrNull(c, "answer", Bottom)), "item", Bottom)))

  private def tolerantItems: Column = columnOrNull("items", Bottom)

  @Test
  def stopsWhereTheStepFallsBackWithTheRowsTheCatchGave(): Unit = {
    val tolerant = nested.select(
      transformTree(tolerantItems, tolerantLinkId, java.util.List.of(tolerantItem), 1)
        .alias("linkIds"))
    val direct = nested.select(
      transformTree(F.col("items"), op(_.getField("linkId")),
        java.util.List.of(op(c => unnest(c.getField("item")))), 1).alias("linkIds"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"), rows(tolerant))
    assertEquals(rows(direct), rows(tolerant))
  }

  @Test
  def stopsWhereTheStepFallsBackEvenWhenDepthExhaustionIsAnError(): Unit = {
    // Before T110a, reaching past the schema was signalled by the catch. Without a signal, the
    // recursion over a fallback would run until the depth counter stopped it, and with
    // errorOnDepthExhaustion that would be an error rather than an empty result.
    val result = nested.select(
      transformTree(tolerantItems, tolerantLinkId, java.util.List.of(tolerantItem), 1, true)
        .alias("linkIds"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"), rows(result))
  }

  @Test
  def aTraversalThatFallsBackStopsAloneBesideOneThatDoesNot(): Unit = {
    // The items carry no answer field, so the second traversal falls back at every level, while
    // the first continues.
    val result = nested.select(
      transformTree(tolerantItems, tolerantLinkId,
        java.util.List.of(tolerantItem, tolerantAnswerItem), 1).alias("linkIds"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"), rows(result))
  }

  @Test
  def anAbsentRootGivesATypedEmptyArrayWhereAnElementTypeIsExpected(): Unit = {
    val typed = rootless.select(
      transformTree(tolerantItems, op(c => F.transform(c, e => F.struct(
        resolveOrNull(e, "linkId", StringType).alias("linkId")))),
        java.util.List.of(tolerantItem), 2, true, LinkIdElement).alias("result"))
    val untyped = rootless.select(
      transformTree(tolerantItems, tolerantLinkId, java.util.List.of(tolerantItem), 2, true)
        .alias("result"))

    assertEquals(ArrayType(LinkIdElement), typed.schema("result").dataType)
    assertEquals(Seq("[ArraySeq()]"), rows(typed))
    assertTrue(UnresolvedTransformTree.isBottom(untyped.schema("result").dataType))
    assertEquals(Seq("[ArraySeq()]"), rows(untyped))
  }

  @Test
  def aBottomTypedLevelNeverReachesTheVariantConversion(): Unit = {
    // The variant form wraps each level's extraction in to_variant_object. The check stops the
    // recursion on the resolved step before the extractor is applied to it, so no conversion is
    // ever applied to a bottom-typed level, at the root or below it.
    val overNested = nested.select(
      variantTransformTree(tolerantItems, op(c => c), java.util.List.of(tolerantItem), 1, true)
        .alias("items"))
    val overRootless = rootless.select(
      variantTransformTree(tolerantItems, op(c => c), java.util.List.of(tolerantItem), 1, true)
        .alias("items"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"),
      rows(overNested.select(F.transform(F.col("items"), _.getField("linkId")))))
    assertEquals(Seq("[ArraySeq()]"), rows(overRootless))
    Seq(overNested, overRootless).foreach { df =>
      val converted = variantConversionInputs(df)
      assertFalse(converted.exists(UnresolvedTransformTree.isBottom), converted.toString)
    }
    // A positive control on the collection of conversion inputs: the populated levels are there.
    assertFalse(variantConversionInputs(overNested).isEmpty)
  }

  /** The types of every input converted to a variant in the analyzed plan. */
  private def variantConversionInputs(df: DataFrame): Seq[DataType] =
    df.queryExecution.analyzed.expressions
      .flatMap(_.collect { case e: Expression if e.prettyName == "to_variant_object" => e })
      .map(_.children.head.dataType)
}
