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

import au.csiro.pathling.encoders.ColumnFunctions.{instantColumnOrNull, resolveOrNull}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests for the normalisation of the previous layout's instants, which it stores as timestamps, to
 * the new layout's text (decision 81).
 *
 * The text is the point in time in UTC, with a `Z` suffix and with fractional seconds only where
 * they are not zero. It must not depend on the session time zone. Every column under test is built
 * with no reference to any dataset, as the engine builds them, and applied to data read back from
 * Parquet, so that the optimiser does not fold the plan into a local relation.
 */
class InstantNormalisationTest extends SparkSessionSupport {

  // Instants at the root, with whole seconds, fractional seconds, a point before the epoch and a
  // null. Each literal carries its offset, so the stored point in time does not depend on the
  // session time zone.
  private def root: DataFrame = parquet("root",
    """select 1 as id, timestamp'2023-01-01 12:00:00+10:00' as issued
      |union all select 2, timestamp'2023-01-01 02:00:00.123Z'
      |union all select 3, timestamp'2023-01-01 02:00:00.120Z'
      |union all select 4, timestamp'2023-01-01 02:00:00.000001Z'
      |union all select 5, timestamp'1969-12-31 23:59:59.5Z'
      |union all select 6, cast(null as timestamp)""".stripMargin)

  // Instants inside structures: a singular one beside a string, a repeating one, and a repeating
  // parent whose elements each hold one.
  private def nested: DataFrame = parquet("nested",
    """select 1 as id,
      |  named_struct('lastUpdated', timestamp'2023-01-01 12:00:00.250+10:00',
      |    'versionId', '1') as meta,
      |  named_struct('times', array(timestamp'2023-01-01 02:00:00Z',
      |    cast(null as timestamp), timestamp'2023-01-01 02:00:01.5Z')) as schedule,
      |  array(named_struct('when', timestamp'2023-01-01 02:00:00Z'),
      |    named_struct('when', timestamp'2024-02-29 23:59:59.999999Z')) as signature
      |union all
      |select 2, named_struct('lastUpdated', cast(null as timestamp), 'versionId', '2'),
      |  named_struct('times', cast(null as array<timestamp>)),
      |  cast(null as array<struct<when:timestamp>>)""".stripMargin)

  private val expectedRoot = Seq(
    "[1,2023-01-01T02:00:00Z]",
    "[2,2023-01-01T02:00:00.123Z]",
    "[3,2023-01-01T02:00:00.12Z]",
    "[4,2023-01-01T02:00:00.000001Z]",
    "[5,1969-12-31T23:59:59.5Z]",
    "[6,null]")

  @Test
  def rootInstantIsItsUtcText(): Unit = {
    val result = root.select(F.col("id"), instantColumnOrNull("issued", StringType))
    assertEquals(StringType, result.schema(1).dataType)
    assertEquals(expectedRoot, rows(result))
  }

  @Test
  def textDoesNotDependOnTheSessionTimeZone(): Unit = {
    val previous = spark.conf.get("spark.sql.session.timeZone")
    try {
      for (zone <- Seq("Australia/Brisbane", "America/New_York", "UTC")) {
        spark.conf.set("spark.sql.session.timeZone", zone)
        assertEquals(expectedRoot,
          rows(root.select(F.col("id"), instantColumnOrNull("issued", StringType))), zone)
        assertEquals(Seq("[1,2023-01-01T02:00:00.25Z]", "[2,null]"),
          rows(nested.select(F.col("id"),
            resolveOrNull(F.col("meta"), "lastUpdated", StringType))), zone)
      }
    } finally {
      spark.conf.set("spark.sql.session.timeZone", previous)
    }
  }

  @Test
  def singularFieldIsItsUtcTextAndItsSiblingIsUnchanged(): Unit = {
    val result = nested.select(F.col("id"),
      resolveOrNull(F.col("meta"), "lastUpdated", StringType),
      resolveOrNull(F.col("meta"), "versionId", StringType))
    assertEquals(StringType, result.schema(1).dataType)
    assertEquals(StringType, result.schema(2).dataType)
    assertEquals(Seq("[1,2023-01-01T02:00:00.25Z,1]", "[2,null,2]"), rows(result))
  }

  @Test
  def arrayOfInstantsIsAnArrayOfText(): Unit = {
    val result = nested.select(F.col("id"),
      resolveOrNull(F.col("schedule"), "times", ArrayType(StringType)))
    assertEquals(ArrayType(StringType, containsNull = true), result.schema(1).dataType)
    assertEquals(
      Seq("[1,ArraySeq(2023-01-01T02:00:00Z, null, 2023-01-01T02:00:01.5Z)]", "[2,null]"),
      rows(result))
  }

  @Test
  def instantUnderRepeatingParentIsAnArrayOfText(): Unit = {
    val result = nested.select(F.col("id"),
      resolveOrNull(F.col("signature"), "when", ArrayType(StringType)))
    assertEquals(ArrayType(StringType, containsNull = true), result.schema(1).dataType)
    assertEquals(
      Seq("[1,ArraySeq(2023-01-01T02:00:00Z, 2024-02-29T23:59:59.999999Z)]", "[2,null]"),
      rows(result))
  }

  @Test
  def instantInsideALambdaIsItsUtcText(): Unit = {
    val result = nested.select(F.col("id"),
      F.transform(F.col("signature"), s => resolveOrNull(s, "when", StringType)))
    assertEquals(
      Seq("[1,ArraySeq(2023-01-01T02:00:00Z, 2024-02-29T23:59:59.999999Z)]", "[2,null]"),
      rows(result))
  }

  @Test
  def textInstantOfTheNewLayoutIsUnchanged(): Unit = {
    val text = parquet("text",
      "select 1 as id, '2023-01-01T12:00:00+10:00' as issued, " +
        "named_struct('lastUpdated', '2023-01-01T12:00:00.250+10:00') as meta")
    assertEquals(Seq("[1,2023-01-01T12:00:00+10:00,2023-01-01T12:00:00.250+10:00]"),
      rows(text.select(F.col("id"), instantColumnOrNull("issued", StringType),
        resolveOrNull(F.col("meta"), "lastUpdated", StringType))))
  }

  @Test
  def absentInstantIsANullOfTheFallback(): Unit = {
    val result = nested.select(F.col("id"), instantColumnOrNull("issued", StringType),
      resolveOrNull(F.col("meta"), "issued", StringType))
    assertEquals(StringType, result.schema(1).dataType)
    assertEquals(Seq("[1,null,null]", "[2,null,null]"), rows(result))
  }
}
