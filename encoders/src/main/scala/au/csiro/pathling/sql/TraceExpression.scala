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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

/**
 * A Catalyst expression that logs the string representation of each evaluated
 * value via SLF4J, then returns the value unchanged. This implements the
 * FHIRPath trace() function semantics.
 *
 * @param child the child expression whose value is traced
 * @param name  the diagnostic label included in log messages
 */
case class TraceExpression(child: Expression, name: String)
  extends UnaryExpression with CodegenFallback {

  @transient
  private lazy val log = LoggerFactory.getLogger(classOf[TraceExpression])

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  @transient
  private lazy val toScala = CatalystTypeConverters.createToScalaConverter(dataType)

  override def nullSafeEval(value: Any): Any = {
    log.info("[trace:{}] {}", name, toReadableString(toScala(value)))
    value
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
