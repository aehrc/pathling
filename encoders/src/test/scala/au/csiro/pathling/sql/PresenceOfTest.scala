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

import au.csiro.pathling.encoders.ColumnFunctions.presenceOf
import org.apache.spark.sql.catalyst.expressions.{ArrayFilter, ArrayTransform,
  AttributeReference, Expression, GetArrayStructFields}
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests for the array that stands for another in a test of presence or a count, [[PresenceOf]]
 * (decision 143, L2).
 *
 * Each form is checked for its shape, and for having the size and the nullness of the array it
 * stands for in every row of data with an absent array, an empty one, null elements, an element
 * whose fields are all null, and a criteria that is null for an element.
 */
class PresenceOfTest extends SparkSessionSupport {

  private def names: DataFrame = parquet("names",
    """select 1 as id, array(named_struct('use', 'official', 'family', 'A', 'given', array('X')),
      |  named_struct('use', cast(null as string), 'family', 'B', 'given', array('Y'))) as name,
      |  array(named_struct('given', array('X'))) as nested, array('a', 'b') as plain
      |union all
      |select 2, cast(null as array<struct<use: string, family: string, given: array<string>>>),
      |  cast(null as array<struct<given: array<string>>>), cast(null as array<string>)
      |union all
      |select 3, array(cast(null as struct<use: string, family: string, given: array<string>>),
      |  named_struct('use', 'nickname', 'family', cast(null as string), 'given',
      |    cast(null as array<string>))), array(cast(null as struct<given: array<string>>)),
      |  array(cast(null as string))
      |union all
      |select 4, filter(array(named_struct('use', 'x', 'family', 'x', 'given', array('x'))),
      |  n -> false), filter(array(named_struct('given', array('x'))), n -> false),
      |  filter(array('x'), n -> false)
      |union all
      |select 5, array(named_struct('use', cast(null as string), 'family', cast(null as string),
      |  'given', cast(null as array<string>))), array(named_struct('given',
      |  cast(null as array<string>))), array('c')""".stripMargin)

  private def withoutNulls(array: Column): Column = F.filter(array, element => element.isNotNull)

  /** The replacement of the presence array of a column, as the analysed plan resolves it. */
  private def replacementOf(df: DataFrame, array: Column): Expression =
    df.select(presenceOf(array)).queryExecution.analyzed.expressions.head
      .collectFirst { case presence: PresenceOf => presence.replacement }.get

  /** Asserts that the presence array of a column has its size and its nullness in every row. */
  private def assertStandsFor(df: DataFrame, array: Column): Unit = {
    val presence = presenceOf(array)
    val compared = df.select(F.col("id"),
      F.coalesce(F.size(array), F.lit(-2)).alias("size"), array.isNull.alias("absent"),
      F.coalesce(F.size(presence), F.lit(-2)).alias("presenceSize"),
      presence.isNull.alias("presenceAbsent"))
    assertTrue(compared.collect().forall(row => row.get(1) == row.get(3) && row.get(2) == row.get(4)),
      rows(compared).mkString("\n"))
  }

  @Test
  def aCriteriaFilterIsTestedOnWhatTheCriteriaReads(): Unit = {
    val criteria = F.filter(withoutNulls(F.col("name")), n => n.getField("use").equalTo("official"))
    replacementOf(names, criteria) match {
      case ArrayFilter(ArrayTransform(ArrayFilter(_, _), _), _) =>
      case other => fail(s"Unexpected form: $other")
    }
    assertStandsFor(names, criteria)
  }

  @Test
  def aCriteriaThatIsNullForAnElementDoesNotKeepIt(): Unit = {
    // The use of the second name of the first row, and of every name of the fifth, is null.
    val criteria = F.filter(withoutNulls(F.col("name")), n => n.getField("use").notEqual("x"))
    assertStandsFor(names, criteria)
  }

  @Test
  def aCriteriaWithAnIndexIsTestedOnWhatTheCriteriaReads(): Unit = {
    val criteria = F.filter(withoutNulls(F.col("name")),
      (n, i) => n.getField("use").isNotNull.or(i.equalTo(1)))
    replacementOf(names, criteria) match {
      case ArrayFilter(ArrayTransform(_, _), _) =>
      case other => fail(s"Unexpected form: $other")
    }
    assertStandsFor(names, criteria)
  }

  @Test
  def aCriteriaThatReadsAnOuterLambdaVariableStandsForItsFilter(): Unit = {
    // Inside a lambda over the rows' plain values, each name is kept where its use is one of them.
    val counted = F.transform(F.col("plain"), value =>
      F.size(presenceOf(F.filter(F.col("name"), n => n.getField("use").equalTo(value)))))
    val expected = F.transform(F.col("plain"), value =>
      F.size(F.filter(F.col("name"), n => n.getField("use").equalTo(value))))
    val compared = names.select(F.coalesce(counted, F.array()).alias("counted"),
      F.coalesce(expected, F.array()).alias("expected"))
    assertTrue(compared.collect().forall(row => row.get(0) == row.get(1)),
      rows(compared).mkString("\n"))
  }

  @Test
  def aNullRemovalIsTestedOnTheFirstAtomicField(): Unit = {
    replacementOf(names, withoutNulls(F.col("name"))) match {
      case fields: GetArrayStructFields => assertEquals("use", fields.field.name)
      case other => fail(s"Unexpected form: $other")
    }
    assertStandsFor(names, withoutNulls(F.col("name")))
  }

  @Test
  def anArrayOfStructuresIsTestedOnTheFirstAtomicField(): Unit = {
    replacementOf(names, F.col("name")) match {
      case fields: GetArrayStructFields => assertEquals("use", fields.field.name)
      case other => fail(s"Unexpected form: $other")
    }
    assertStandsFor(names, F.col("name"))
  }

  @Test
  def anArrayWithNoAtomicFieldOrOfPlainValuesStandsAsItself(): Unit = {
    assertTrue(replacementOf(names, withoutNulls(F.col("nested"))).isInstanceOf[ArrayFilter])
    assertStandsFor(names, withoutNulls(F.col("nested")))
    assertTrue(replacementOf(names, F.col("plain")).isInstanceOf[AttributeReference])
    assertStandsFor(names, F.col("plain"))
  }
}
