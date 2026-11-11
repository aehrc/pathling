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

import au.csiro.pathling.encoders.ColumnFunctions.{annotationOrElse, resolveOrNone}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Tests for the deferred projection of annotation fields, [[ResolveOrNone]], and for the accessor
 * of an annotation, [[AnnotationOrElse]] (design §4.3).
 *
 * Every column is built with no reference to the data, and applied to data whose metadata group
 * carries both annotations, one of them, or none, so that the shape is seen to follow the schema.
 */
class ResolveOrNoneTest extends SparkSessionSupport {

  private val Fields = Seq("__start", "__end")

  private def both: DataFrame = parquet("both",
    """select 1 as id, 'a' as v, named_struct('id', 'x', '__start', 10, '__end', 19) as g,
      |  array('b', 'c', 'd') as vs,
      |  array(named_struct('__start', 20, '__end', 29), named_struct('__start', 30, '__end', 39))
      |    as gs
      |union all
      |select 2, cast(null as string), cast(null as struct<id: string, __start: int, __end: int>),
      |  array('e'), cast(null as array<struct<__start: int, __end: int>>)""".stripMargin)

  private def onlyStart: DataFrame = parquet("onlyStart",
    """select 1 as id, 'a' as v, named_struct('id', 'x', '__start', 10) as g, array('b') as vs,
      |  array(named_struct('__start', 20)) as gs""".stripMargin)

  private def none: DataFrame = parquet("none",
    "select 1 as id, 'a' as v, array('b') as vs, cast(null as string) as other")

  private def element(group: Column): Column =
    resolveOrNone(group, Fields.asJava, Seq("value").asJava, Seq(F.col("v")).asJava, false)

  private def elements(groups: Column): Column =
    resolveOrNone(groups, Fields.asJava, Seq("value").asJava, Seq(F.col("vs")).asJava, true)

  private def typeOf(df: DataFrame): String = df.schema.head.dataType.simpleString

  @Test
  def singularTakesThePresentFieldsOnlyAndDropsTheRest(): Unit = {
    val withBoth = both.select(element(F.col("g")).alias("e"))
    assertEquals("struct<value:string,__start:int,__end:int>", typeOf(withBoth))
    assertEquals(Seq("[[a,10,19]]", "[[null,null,null]]"), rows(withBoth))
    val withStart = onlyStart.select(element(F.col("g")).alias("e"))
    assertEquals("struct<value:string,__start:int>", typeOf(withStart))
    assertEquals(Seq("[[a,10]]"), rows(withStart))
  }

  @Test
  def singularOverAnAbsentSourceIsTheContentAlone(): Unit = {
    val result = none.select(element(F.lit(null)).alias("e"))
    assertEquals("struct<value:string>", typeOf(result))
    assertEquals(Seq("[[a]]"), rows(result))
  }

  @Test
  def repeatingPairsByPositionWithTheSingularNames(): Unit = {
    val result = both.select(F.col("id"), elements(F.col("gs")).alias("e"))
    assertEquals("array<struct<value:string,__start:int,__end:int>>",
      result.schema("e").dataType.simpleString)
    // A shorter array of groups pads the annotations with nulls, and an absent one does not null
    // the pairing.
    assertEquals(Seq("[1,ArraySeq([b,20,29], [c,30,39], [d,null,null])]",
      "[2,ArraySeq([e,null,null])]"), rows(result))
  }

  @Test
  def repeatingOverAbsentAnnotationsHasTheSameNames(): Unit = {
    val withStart = onlyStart.select(elements(F.col("gs")).alias("e"))
    assertEquals("array<struct<value:string,__start:int>>", typeOf(withStart))
    val withNone = none.select(elements(F.lit(null)).alias("e"))
    assertEquals("array<struct<value:string>>", typeOf(withNone))
    assertEquals(Seq("[ArraySeq([b])]"), rows(withNone))
  }

  @Test
  def accessorReadsTheStoredFieldOrComputes(): Unit = {
    val guarded = F.when(F.col("v").isNotNull, element(F.col("g")))
    val start = annotationOrElse(guarded, "__start", F.lit(-1))
    val end = annotationOrElse(guarded, "__end", F.lit(-2))
    assertEquals(Seq("[1,10,19]", "[2,null,null]"), rows(both.select(F.col("id"), start, end)))
    assertEquals(Seq("[10,-2]"), rows(onlyStart.select(start, end)))
    // Inside a lambda the element is a variable, and the accessor resolves on its type.
    val each = F.transform(elements(F.col("gs")), e => annotationOrElse(e, "__end", F.lit(-2)))
    assertEquals(Seq("[1,ArraySeq(29, 39, null)]", "[2,ArraySeq(null)]"),
      rows(both.select(F.col("id"), each)))
    assertEquals(Seq("[ArraySeq(-2)]"), rows(onlyStart.select(each)))
  }

  /**
   * The accessor reads a stored annotation past the traversal's guard on the value, which relies
   * on the trust contract: an annotation is null exactly where its value is (ticket 31). Data that
   * breaks it, with an annotation beside an absent value, gives that annotation, where the guarded
   * element itself is absent. This is what lets a filter on a stored bound be pushed down.
   */
  @Test
  def accessorReliesOnAnAnnotationBeingNullWhereItsValueIs(): Unit = {
    val untrusted = parquet("untrusted",
      "select cast(null as string) as v, named_struct('__start', 10, '__end', 19) as g")
    val guarded = F.when(F.col("v").isNotNull, element(F.col("g")))
    assertEquals(Seq("[null,10]"),
      rows(untrusted.select(guarded, annotationOrElse(guarded, "__start", F.lit(-1)))))
  }

  @Test
  def accessorDoesNotSeeThroughAGuardOnSomethingElse(): Unit = {
    // A guard that tests something other than the element's value can be false where the value is
    // present, so the annotation is null there, as the element is.
    val unrelated = F.when(F.try_element_at(F.col("vs"), F.lit(4)).isNotNull, element(F.col("g")))
    assertEquals(Seq("[1,null]", "[2,null]"),
      rows(both.select(F.col("id"), annotationOrElse(unrelated, "__start", F.lit(-1)))))
    val absent = F.when(F.lit(null).isNotNull, element(F.col("g")))
    assertEquals(Seq("[1,null]", "[2,null]"),
      rows(both.select(F.col("id"), annotationOrElse(absent, "__start", F.lit(-1)))))
  }

  @Test
  def accessorOfAGuardedElementPushesAFilterOnTheStoredField(): Unit = {
    val guarded = F.when(F.col("v").isNotNull, element(F.col("g")))
    val query = both.filter(annotationOrElse(guarded, "__end", F.lit(-2)) >= 15)
    assertTrue(pushedFilters(query).contains("GreaterThanOrEqual(g.__end,15)"), pushedFilters(query))
    assertEquals(Seq("[1]"), rows(query.select("id")))
    // The control: a plain field read through the guard is not pushed.
    val control = both.filter(guarded.getField("__end") >= 15)
    assertFalse(pushedFilters(control).contains("__end"), pushedFilters(control))
  }

  private def pushedFilters(query: DataFrame): String =
    query.queryExecution.executedPlan.collectLeaves()
      .collectFirst { case scan: FileSourceScanExec => scan.metadata.getOrElse("PushedFilters", "") }
      .getOrElse("")
}
