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
import au.csiro.pathling.encoders.ValueFunctions.{transformTree, unnest}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.read.ListAppender
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import java.util.function.UnaryOperator
import scala.jdk.CollectionConverters._

/**
 * Tests that the recursive tree traversal evaluates a non-deterministic node once (#2594). Each
 * level reads its node once for extraction and again within every deeper level, so a node that
 * traces its value would otherwise record it once per read.
 *
 * @author John Grimes
 */
class TransformTreeTraceTest extends SparkSessionSupport {

  private val Bottom: DataType = ArrayType(NullType)

  private val traceLogger: Logger = LoggerFactory.getLogger(classOf[TraceExpression])
    .asInstanceOf[Logger]

  private var appender: ListAppender[ILoggingEvent] = _

  private var originalLevel: Level = _

  // Three levels of items, where the deepest level has no item field, as the encoder's nesting
  // bound leaves it.
  private def nested: DataFrame = parquet("nested",
    """select 1 as id, array(named_struct('linkId', '1', 'text', 'L0', 'item',
      |  array(named_struct('linkId', '2', 'text', 'L1', 'item',
      |    array(named_struct('linkId', '3', 'text', 'L2')))))) as items""".stripMargin)

  @BeforeEach
  def attachAppender(): Unit = {
    originalLevel = traceLogger.getLevel
    traceLogger.setLevel(Level.TRACE)
    appender = new ListAppender[ILoggingEvent]()
    appender.start()
    traceLogger.addAppender(appender)
  }

  @AfterEach
  def detachAppender(): Unit = {
    traceLogger.detachAppender(appender)
    traceLogger.setLevel(originalLevel)
    appender.stop()
  }

  private def op(f: Column => Column): UnaryOperator[Column] = (c: Column) => f(c)

  private def traced(value: Column, label: String): Column =
    column(TraceExpression(expression(value), label, "item", null))

  private def tolerantLinkId: UnaryOperator[Column] = op(resolveOrNull(_, "linkId", StringType))

  private def tolerantItem: UnaryOperator[Column] = op(c => unnest(resolveOrNull(c, "item", Bottom)))

  private def fires(label: String): Int =
    appender.list.asScala.count(_.getFormattedMessage.startsWith(s"[trace:$label]"))

  @Test
  def aTracedRootIsEvaluatedOnce(): Unit = {
    val result = nested.select(
      transformTree(traced(columnOrNull("items", Bottom), "root"), tolerantLinkId,
        java.util.List.of(tolerantItem), 1).alias("linkIds"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"), rows(result))
    assertEquals(1, fires("root"))
  }

  @Test
  def aTracedTraversalIsEvaluatedOncePerLevel(): Unit = {
    // The traversal reaches the two levels below the root. The step below the deepest level walks
    // off the schema, so it is never evaluated.
    val result = nested.select(
      transformTree(columnOrNull("items", Bottom), tolerantLinkId,
        java.util.List.of(op(c => traced(tolerantItem(c), "step"))), 1).alias("linkIds"))

    assertEquals(Seq("[ArraySeq(1, 2, 3)]"), rows(result))
    assertEquals(2, fires("step"))
  }
}
