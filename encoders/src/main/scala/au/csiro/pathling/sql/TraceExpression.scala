/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

/**
 * A Catalyst expression that returns one value (left) while logging another
 * (right) via SLF4J. This implements the FHIRPath trace() function semantics,
 * including the optional projection argument.
 *
 * When no projection is provided, both children reference the same column, so
 * the logged value equals the returned value.
 *
 * When a [[TraceCollector]] is provided, each value is also added to the
 * collector with the trace label and FHIR type, enabling programmatic capture
 * of trace output.
 *
 * This expression is marked [[Nondeterministic]] because it has side effects
 * (logging, collector accumulation) that must not be eliminated by the Catalyst
 * optimizer.
 *
 * Null handling is asymmetric: a null left (pass-through) value returns null
 * without logging. A null right (projected) value skips logging but still
 * returns the left value.
 *
 * @param left      the pass-through expression whose value is returned
 * @param right     the expression whose value is logged
 * @param name      the diagnostic label included in log messages
 * @param fhirType  the FHIR type code of the logged expression (e.g., "string")
 * @param collector an optional collector for programmatic trace capture, or null
 */
case class TraceExpression(left: Expression, right: Expression, name: String,
                           fhirType: String, collector: TraceCollector)
  extends BinaryExpression with CodegenFallback with Nondeterministic {

  @transient
  private lazy val log = LoggerFactory.getLogger(classOf[TraceExpression])

  override def dataType: DataType = left.dataType

  override def nullable: Boolean = left.nullable

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)

  override protected def initializeInternal(partitionIndex: Int): Unit = ()

  @transient
  private lazy val toScala = CatalystTypeConverters.createToScalaConverter(right.dataType)

  override protected def evalInternal(input: InternalRow): Any = {
    val passThrough = left.eval(input)
    if (passThrough == null) {
      return null
    }
    val toLog = right.eval(input)
    if (toLog != null && (log.isTraceEnabled || collector != null)) {
      val converted = toScala(toLog)
      if (log.isTraceEnabled) {
        log.trace("[trace:{}] {}", name, toReadableString(converted))
      }
      if (collector != null) {
        collector.add(name, fhirType, converted)
      }
    }
    passThrough
  }

  private def toReadableString(value: Any): String = value match {
    case row: Row => row.json
    case seq: scala.collection.Seq[_] => seq.map(toReadableString).mkString("[", ", ", "]")
    case s: String => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case null => "null"
    case other => String.valueOf(other)
  }

  override def prettyName: String = "trace"
}
