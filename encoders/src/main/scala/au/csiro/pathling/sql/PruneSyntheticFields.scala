/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * An expression that removes all fields starting with '_' underscores from struct values.
 * Other types are not affected.
 *
 * @param child the child expression
 */
case class PruneSyntheticFields(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  @transient
  lazy val inputSchema: DataType = child.dataType

  override def dataType: DataType = {
    inputSchema match {
      case structType: StructType => StructType(
        structType.fields.filter(sf => !isAnnotation(sf.name)))
      case _ => inputSchema
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = PruneSyntheticFields(
    newChild)

  override def nullSafeEval(value: Any): Any = {
    inputSchema match {
      case _: StructType =>
        val row = value.asInstanceOf[InternalRow]
        val structSchema = inputSchema.asInstanceOf[StructType]
        val normalizedValues = structSchema.fieldNames.zip(row.toSeq(structSchema))
          .filter {
            case (name, _) => !isAnnotation(name)
          }
          .map(_._2)
        InternalRow(normalizedValues.toIndexedSeq: _*)
      case _ => value
    }
  }

  override def prettyName: String = "pruneSyntheticFields"

  def isAnnotation(name: String): Boolean = {
    name.startsWith("_") || // the typical prefix for synthetic fields
      name.endsWith("_scale") // the scale field added for decimal representation
  }
}
