/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * An expression that removes all fields starting with '_' underscores from struct values.
 * Other types are not affected.
 *
 * @param child the child expresion.
 */
case class PruneSyntheticFields(child: Expression) extends UnaryExpression with CodegenFallback with NullIntolerant {
  override def nullable: Boolean = true

  @transient
  lazy val inputSchema: DataType = child.dataType

  override def dataType: DataType = {
    inputSchema match {
      case structType: StructType => StructType(structType.fields.filter(!_.name.startsWith("_")))
      case _ => inputSchema
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = PruneSyntheticFields(newChild)

  override def nullSafeEval(value: Any): Any = {
    inputSchema match {
      case _: StructType =>
        val row = value.asInstanceOf[InternalRow]
        val structSchema = inputSchema.asInstanceOf[StructType]
        val normalizedValues = structSchema.fieldNames.zip(row.toSeq(structSchema))
          .filter {
            case (name, value) => !name.startsWith("_")
          }
          .map(_._2)
        InternalRow(normalizedValues: _*)
      case _ => value
    }
  }

  override def prettyName: String = "pruneSyntheticFields"
}
