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

import au.csiro.pathling.encoders.ColumnFunctions
import au.csiro.pathling.utilities.{CanonicalStructure, StructureMerge}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * A canonical structure built by hand, so that these tests do not depend on the definitions.
 *
 * @param order    the canonical field order at this node
 * @param children the structures beneath the fields that have one
 */
case class TestCanonicalStructure(order: Seq[String],
    children: Map[String, TestCanonicalStructure] = Map.empty) extends CanonicalStructure {

  override def fieldOrder(): java.util.List[String] = order.asJava

  override def field(name: String): Optional[CanonicalStructure] =
    children.get(name).map(c => Optional.of[CanonicalStructure](c))
      .getOrElse(Optional.empty[CanonicalStructure]())
}

/**
 * Tests for the reconciliation expression (T038j and T038k): collections of the same FHIR type
 * whose SQL shapes differ are projected by name into their merged type.
 */
class MergeCastTest extends SparkSessionSupport {

  // The canonical structure of a HumanName-like type: definition order, with a nested Period.
  private val Name = TestCanonicalStructure(
    Seq("id", "use", "family", "given", "period"),
    Map("period" -> TestCanonicalStructure(Seq("start", "end"))))

  // Three fitted shapes of the same type, each reached by a different path.
  private def data: DataFrame = parquet("names",
    """select 1 as id,
      |  array(named_struct('id', 'a1', 'family', 'Smith'),
      |    cast(null as struct<id: string, family: string>)) as a,
      |  array(named_struct('id', 'b1', 'given', array('John'),
      |    'period', named_struct('end', '2020'))) as b,
      |  array(named_struct('use', 'official', 'period', named_struct('start', '2010'))) as c,
      |  named_struct('id', 'a1', 'family', 'Smith') as sa,
      |  named_struct('id', 'b1', 'given', array('John')) as sb,
      |  cast(null as struct<id: string, family: string>) as sn""".stripMargin)

  private def mergeCast(index: Int, operands: Column*): Column =
    ColumnFunctions.mergeCast(operands.asJava, index, Name)

  private def all(operands: Column*): Seq[Column] = operands.indices.map(i => mergeCast(i, operands: _*))

  private def typeOf(df: DataFrame, name: String): DataType = df.schema(name).dataType

  // -----------------------------------------------------------------------------------------------
  // T038j: by-name projection into the merged type.
  // -----------------------------------------------------------------------------------------------

  @Test
  def differentShapesProjectIntoMergedTypeAndCombine(): Unit = {
    // Without reconciliation the two shapes do not combine.
    val error = assertThrows(classOf[AnalysisException],
      () => data.select(F.concat(F.col("a"), F.col("b"))).collect())
    assertTrue(error.getCondition.startsWith("DATATYPE_MISMATCH"), error.getCondition)

    val combined = data.select(F.concat(all(F.col("a"), F.col("b")): _*).alias("n"))
    val expected = ArrayType(StructureMerge.merge(Seq(
      elementOf(typeOf(data, "a")), elementOf(typeOf(data, "b"))).asJava, Name))
    assertEquals(expected, typeOf(combined, "n"))
    assertEquals(Seq("id", "family", "given", "period"),
      elementOf(typeOf(combined, "n")).fieldNames.toSeq)
    // Fields a side lacks are null, and a null element stays null rather than becoming a
    // structure of nulls. Only `b` has a period, so the merged period has only `end`.
    assertEquals(Seq("[ArraySeq([a1,Smith,null,null], null, [b1,null,ArraySeq(John),[2020]])]"),
      rows(combined))
  }

  @Test
  def resultIsTraversable(): Unit = {
    val combined = F.concat(all(F.col("a"), F.col("b"), F.col("c")): _*)
    assertEquals(Seq("[ArraySeq(Smith, null, null, null),ArraySeq(null, null, 2020, null)]"),
      rows(data.select(combined.getField("family"), combined.getField("period").getField("end"))))
    assertEquals(Seq("[ArraySeq(null, null, null, 2010)]"),
      rows(data.select(combined.getField("period").getField("start"))))
  }

  @Test
  def nestedStructuresAreOrderedCanonically(): Unit = {
    // `b` discovers `end` in its period, and `c` discovers `start`: the merged period is in
    // canonical order whichever operand comes first.
    for (operands <- Seq(Seq(F.col("b"), F.col("c")), Seq(F.col("c"), F.col("b")))) {
      val combined = data.select(F.concat(all(operands: _*): _*).alias("n"))
      val period = elementOf(typeOf(combined, "n"))("period").dataType.asInstanceOf[StructType]
      assertEquals(Seq("start", "end"), period.fieldNames.toSeq)
      assertEquals(Seq("id", "use", "given", "period"),
        elementOf(typeOf(combined, "n")).fieldNames.toSeq)
    }
  }

  @Test
  def singularStructuresProjectIntoMergedType(): Unit = {
    val Seq(sa, sb) = all(F.col("sa"), F.col("sb"))
    val result = data.select(F.array(sa, sb).alias("n"))
    assertEquals(Seq("id", "family", "given"), elementOf(typeOf(result, "n")).fieldNames.toSeq)
    assertEquals(Seq("[ArraySeq([a1,Smith,null], [b1,null,ArraySeq(John)])]"), rows(result))
    // A null structure stays null.
    val Seq(sn, _) = all(F.col("sn"), F.col("sb"))
    assertEquals(Seq("[null]"), rows(data.select(sn)))
  }

  @Test
  def absentOperandTakesTheMergedType(): Unit = {
    // An absent operand is typed as the bottom type, and takes the type of the others.
    val absent = F.lit(null).cast(ArrayType(NullType))
    val Seq(n, a) = all(absent, F.col("a"))
    val result = data.select(n.alias("n"), a.alias("a"))
    assertEquals(typeOf(result, "a"), typeOf(result, "n"))
    assertEquals(Seq("[null,ArraySeq([a1,Smith], null)]"), rows(result))
  }

  @Test
  def projectionIsByNameAndNotACast(): Unit = {
    // Two structures with the same field types in a different order. A cast between them
    // reorders positionally, with no error, and swaps the values.
    val swapped = parquet("swapped",
      """select named_struct('family', 'Smith', 'given', 'John') as x,
        |  named_struct('given', 'Jane', 'family', 'Jones') as y""".stripMargin)
    val cast = swapped.select(F.col("y").cast(typeOf(swapped, "x")).getField("family"))
    assertEquals(Seq("[Jane]"), rows(cast))

    val Seq(x, y) = all(F.col("x"), F.col("y"))
    val result = swapped.select(F.array(x, y).alias("n"))
    assertEquals(Seq("family", "given"), elementOf(typeOf(result, "n")).fieldNames.toSeq)
    assertEquals(Seq("[ArraySeq(Smith, Jones),ArraySeq(John, Jane)]"),
      rows(result.select(F.col("n").getField("family"), F.col("n").getField("given"))))
    // Nor does the optimised plan contain a cast.
    val plan = result.queryExecution.optimizedPlan.toString
    assertFalse(plan.toLowerCase.contains("cast("), plan)
  }

  @Test
  def operandsThatAlreadyAgreeAreLeftAsTheyAre(): Unit = {
    // Two structures of one type, in an order that is not canonical. There is nothing to
    // reconcile, so neither is reordered, and the expression reduces to the operands themselves.
    val agreeing = parquet("agreeing",
      """select named_struct('given', array('John'), 'family', 'Smith') as x,
        |  named_struct('given', array('Jane'), 'family', 'Jones') as y""".stripMargin)
    val Seq(x, y) = all(F.col("x"), F.col("y"))
    val result = agreeing.select(x.alias("x"), y.alias("y"))
    assertEquals(typeOf(agreeing, "x"), typeOf(result, "x"))
    assertEquals(typeOf(agreeing, "y"), typeOf(result, "y"))
    assertEquals(Seq("given", "family"), elementOf(typeOf(result, "x")).fieldNames.toSeq)
    assertEquals(Seq("[[ArraySeq(John),Smith],[ArraySeq(Jane),Jones]]"), rows(result))
  }

  @Test
  def operandsThatCannotBeMergedFailAnalysis(): Unit = {
    // A singular structure and an array of structures have no merged type.
    val mixed = assertThrows(classOf[AnalysisException],
      () => data.select(mergeCast(0, F.col("sa"), F.col("a"))).collect())
    assertTrue(mixed.getCondition.startsWith("DATATYPE_MISMATCH"), mixed.getCondition)
    // Nor do structures that disagree on the type of a field.
    val disagreeing = assertThrows(classOf[AnalysisException],
      () => data.select(mergeCast(0, F.col("sa"), F.struct(F.lit(1).alias("id")))).collect())
    assertTrue(disagreeing.getCondition.startsWith("DATATYPE_MISMATCH"),
      disagreeing.getCondition)
  }

  // -----------------------------------------------------------------------------------------------
  // T038k: folding the binary form agrees with one variadic call (finding 16).
  // -----------------------------------------------------------------------------------------------

  @Test
  def foldingBinaryFormMatchesVariadicCall(): Unit = {
    val (a, b, c) = (F.col("a"), F.col("b"), F.col("c"))
    val variadic = F.concat(all(a, b, c): _*)
    val ab = F.concat(all(a, b): _*)
    val foldedLeft = F.concat(all(ab, c): _*)
    val bc = F.concat(all(b, c): _*)
    val foldedRight = F.concat(all(a, bc): _*)
    val result = data.select(variadic.alias("v"), foldedLeft.alias("l"), foldedRight.alias("r"))
    assertEquals(typeOf(result, "v"), typeOf(result, "l"))
    assertEquals(typeOf(result, "v"), typeOf(result, "r"))
    val Seq(row) = result.collect().toSeq
    assertEquals(row.get(0), row.get(1))
    assertEquals(row.get(0), row.get(2))
  }

  @Test
  def foldingSingularFormMatchesVariadicCallInAnyOrder(): Unit = {
    val (sa, sb, sn) = (F.col("sa"), F.col("sb"), F.col("sn"))
    val variadic = F.array(all(sa, sb, sn): _*)
    val folded = F.array(all(F.array(all(sn, sb): _*).getItem(1), sa): _*)
    val result = data.select(variadic.alias("v"), folded.alias("f"))
    assertEquals(typeOf(result, "v"), typeOf(result, "f"))
  }

  // -----------------------------------------------------------------------------------------------
  // The combination of projected operands holds each operand once (finding 8).
  // -----------------------------------------------------------------------------------------------

  private def combination(operands: Column*): Column =
    ColumnFunctions.mergeCombination(operands.asJava, Name,
      (projected: java.util.List[Column]) => F.concat(projected.asScala.toSeq: _*))

  @Test
  def combinationMatchesTheCombinationOfEachProjection(): Unit = {
    val operands = Seq(F.col("a"), F.col("b"), F.col("c"))
    val result = data.select(combination(operands: _*).alias("m"),
      F.concat(all(operands: _*): _*).alias("e"))
    assertEquals(typeOf(result, "e"), typeOf(result, "m"))
    val Seq(row) = result.collect().toSeq
    assertEquals(row.get(1), row.get(0))
  }

  @Test
  def combinationHoldsEachOperandOnce(): Unit = {
    val analysed = data.select(combination(F.col("a"), F.col("b")).alias("m"))
      .queryExecution.analyzed
    val references = analysed.expressions.flatMap(_.collect {
      case attribute: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
        attribute.name
    })
    assertEquals(Seq("a", "b"), references)
  }

  @Test
  def combinationOfOperandsThatAgreeAddsNothingToThePlan(): Unit = {
    // Both are selected from one read of the data, so that their attributes are the same.
    val source = data
    val combined = source.select(combination(F.col("a"), F.col("a")).alias("m"))
    val plain = source.select(F.concat(F.col("a"), F.col("a")).alias("m"))
    assertTrue(plain.queryExecution.analyzed.sameResult(combined.queryExecution.analyzed),
      combined.queryExecution.analyzed.toString)
  }

  @Test
  def combinationOfOperandsThatCannotBeMergedFailsAnalysis(): Unit = {
    val error = assertThrows(classOf[AnalysisException],
      () => data.select(combination(F.col("sa"), F.col("a"))).collect())
    assertTrue(error.getCondition.startsWith("DATATYPE_MISMATCH"), error.getCondition)
  }

  private def elementOf(dataType: DataType): StructType = dataType match {
    case ArrayType(struct: StructType, _) => struct
    case struct: StructType => struct
    case other => fail(s"Not a structure: $other")
  }
}
