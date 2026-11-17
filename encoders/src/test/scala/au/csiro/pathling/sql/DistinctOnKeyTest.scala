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

import au.csiro.pathling.encoders.ColumnFunctions.distinctOnKey
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Tests for the comparison key that leaves out another producer's annotation fields,
 * [[StripAnnotations]], and the deduplication by it, [[DistinctOnKey]] (decision 151).
 */
class DistinctOnKeyTest extends SparkSessionSupport {

  private val Recognised = Seq("__start").asJava

  // Names that differ only in a foreign annotation, at the root and nested, with a recognised one.
  private def foreign: DataFrame = parquet("foreign",
    """select array(
      |  named_struct('family', 'A', '__acme', 1,
      |    'period', named_struct('start', '2001', '__start', 1, '__acme', 1)),
      |  named_struct('family', 'A', '__acme', 2,
      |    'period', named_struct('start', '2001', '__start', 1, '__acme', 2)),
      |  named_struct('family', 'B', '__acme', 3,
      |    'period', named_struct('start', '2001', '__start', 1, '__acme', 3))) as names""".stripMargin)

  private def plain: DataFrame = parquet("plain",
    "select array(named_struct('family', 'A'), named_struct('family', 'A')) as names")

  @Test
  def theKeyLeavesOutForeignAnnotationsAtEveryDepthAndKeepsRecognisedOnes(): Unit = {
    val element = foreign.schema("names").dataType.asInstanceOf[ArrayType].elementType
    assertEquals("struct<family:string,period:struct<start:string,__start:int>>",
      StripAnnotations.stripped(element, Recognised.asScala.toSeq).simpleString)
  }

  @Test
  def deduplicationKeepsTheFirstElementOfEachKey(): Unit = {
    val names = F.col("names")
    val result = foreign.select(distinctOnKey(names, Recognised).alias("d"))
    // The first element of each key is kept as it is, with its foreign fields.
    assertEquals(Seq("[ArraySeq([A,1,[2001,1,1]], [B,3,[2001,1,3]])]"), rows(result))
  }

  @Test
  def withNothingToLeaveOutTheKeyAndTheDeduplicationAreThePlainOnes(): Unit = {
    val names = F.col("names")
    val keyed = plain.select(distinctOnKey(names, Recognised).alias("d"))
    val control = plain.select(F.array_distinct(names).alias("d"))
    assertTrue(control.queryExecution.optimizedPlan.sameResult(keyed.queryExecution.optimizedPlan),
      keyed.queryExecution.optimizedPlan.toString)
  }

  // Empty and null arrays, and null elements, of a type with a foreign annotation (the keyed
  // branch) and of one without (the plain branch).
  private def edges: DataFrame = parquet("edges",
    """select 1 as id,
      |  cast(array() as array<struct<family: string, __acme: int>>) as keyed,
      |  cast(array() as array<struct<family: string>>) as plain
      |union all
      |select 2, cast(null as array<struct<family: string, __acme: int>>),
      |  cast(null as array<struct<family: string>>)
      |union all
      |select 3,
      |  array(null, named_struct('family', 'A', '__acme', 1), null,
      |    named_struct('family', 'A', '__acme', 2)),
      |  array(null, named_struct('family', 'A'), null, named_struct('family', 'A'))""".stripMargin)

  @Test
  def emptyAndNullArraysAndNullElementsDeduplicateAsArrayDistinctDoes(): Unit = {
    val keyed = edges.select(F.col("id"), distinctOnKey(F.col("keyed"), Recognised).alias("d"))
    assertEquals(Seq("[1,ArraySeq()]", "[2,null]", "[3,ArraySeq(null, [A,1])]"), rows(keyed))
    val plain = edges.select(F.col("id"), distinctOnKey(F.col("plain"), Recognised).alias("d"))
    assertEquals(Seq("[1,ArraySeq()]", "[2,null]", "[3,ArraySeq(null, [A])]"), rows(plain))
    assertEquals(rows(plain),
      rows(edges.select(F.col("id"), F.array_distinct(F.col("plain")).alias("d"))))
  }
}
