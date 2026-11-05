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

import au.csiro.pathling.encoders.ColumnFunctions.{columnOrNull, resolveFieldsOrElse}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.{List => JList}

/**
 * Tests the multi-field tolerant reference: a structure of the stored fields where the schema
 * carries all of them, and the fallback where it lacks any, decided from the schema rather than
 * per row. The column is built with no reference to any dataset, as the engine builds it.
 */
class ResolveFieldsOrElseTest extends SparkSessionSupport {

  private def range: Column = resolveFieldsOrElse(
    F.col("g"),
    JList.of("a", "b"),
    F.struct(F.lit(-1).alias("a"), F.lit(-2).alias("b")))

  @Test
  def takesTheStoredFieldsWhereTheSchemaCarriesAll(): Unit = {
    val stored = parquet("all",
      """select 1 as id, named_struct('x', 0, 'a', 1, 'b', 2) as g
        |union all
        |select 2, cast(null as struct<x int, a int, b int>)""".stripMargin)
    assertEquals(Seq("[1,[1,2]]", "[2,[null,null]]"),
      rows(stored.select(F.col("id"), range)))
  }

  @Test
  def takesTheFallbackWhereTheSchemaLacksAField(): Unit = {
    val partial = parquet("partial", "select 1 as id, named_struct('a', 1) as g")
    assertEquals(Seq("[1,[-1,-2]]"), rows(partial.select(F.col("id"), range)))
  }

  @Test
  def takesTheFallbackWhereTheSchemaLacksTheStructure(): Unit = {
    val absent = parquet("absent", "select 1 as id")
    val fallback = resolveFieldsOrElse(
      columnOrNull("g", DataTypes.NullType), JList.of("a", "b"),
      F.struct(F.lit(-1).alias("a"), F.lit(-2).alias("b")))
    assertEquals(Seq("[1,[-1,-2]]"), rows(absent.select(F.col("id"), fallback)))
  }

  @Test
  def takesTheStoredFieldsOfEachElementOfAnArray(): Unit = {
    val stored = parquet("array",
      "select 1 as id, array(named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)) as g")
    val fallback = F.array(F.struct(F.lit(-1).alias("a"), F.lit(-2).alias("b")))
    assertEquals(Seq("[1,ArraySeq([1,2], [3,4])]"),
      rows(stored.select(F.col("id"), resolveFieldsOrElse(F.col("g"), JList.of("a", "b"), fallback))))
  }
}
