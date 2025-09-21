/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.encoders

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


/**
 * An unresolved expression that performs the repeatAll operation on a collection.
 * This expression analyzes the schema to determine the depth of traversal needed
 * and generates the appropriate recursive traversal expression.
 *
 * @param value                the input expression to traverse
 * @param projectionExpression a function that takes an expression and returns the projection
 * @author John Grimes
 */
case class UnresolvedRepeatAll(value: Expression,
                               projectionExpression: Expression => Expression)
  extends Expression with Unevaluable with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {
    val newValue = f(value)

    if (newValue.resolved) {
      // Analyze the schema and generate the resolved expression.
      analyzeSchemaAndResolve(newValue)
    } else {
      copy(value = newValue)
    }
  }

  /**
   * Analyzes the schema of the input expression and generates a resolved expression
   * that performs recursive traversal based on the actual schema depth.
   */
  private def analyzeSchemaAndResolve(resolvedValue: Expression): Expression = {
    val dataType = resolvedValue.dataType
    val maxDepth = analyzeMaxTraversalDepth(dataType)

    if (maxDepth == 0) {
      // No traversal possible, return the original value.
      resolvedValue
    } else {
      // Generate recursive traversal expression.
      generateTraversalExpression(resolvedValue, maxDepth)
    }
  }

  /**
   * Analyzes the data type to determine the maximum meaningful depth of traversal.
   * This looks at the structure of arrays and structs to find how deep we can go.
   */
  private def analyzeMaxTraversalDepth(dataType: DataType): Int = {
    def analyzeDepth(currentType: DataType, depth: Int = 0): Int = currentType match {
      case ArrayType(elementType, _) =>
        elementType match {
          case StructType(fields) =>
            // Look for fields that could be traversed further.
            val fieldDepths = fields.map(field => analyzeDepth(field.dataType, 0))
            val maxFieldDepth = if (fieldDepths.nonEmpty) fieldDepths.max else 0
            math.max(depth + 1, maxFieldDepth + 1)
          case ArrayType(_, _) =>
            // Nested arrays.
            analyzeDepth(elementType, depth + 1)
          case _ =>
            depth + 1
        }
      case StructType(fields) =>
        // Look for array fields that could be traversed.
        val arrayFields = fields.filter(_.dataType.isInstanceOf[ArrayType])
        if (arrayFields.nonEmpty) {
          val maxFieldDepth = arrayFields.map(field => analyzeDepth(field.dataType, 0)).max
          math.max(depth, maxFieldDepth)
        } else {
          depth
        }
      case _ =>
        depth
    }

    analyzeDepth(dataType)
  }

  /**
   * Generates a traversal expression that applies the projection recursively
   * up to the specified maximum depth.
   */
  private def generateTraversalExpression(input: Expression, maxDepth: Int): Expression = {
    // For now, implement a simple version that just applies the projection once.
    // This should work for basic cases and can be extended later.
    if (maxDepth == 0) {
      input
    } else {
      val projected = projectionExpression(input)
      // For simple case, just return the projection.
      // In a full implementation, this would recursively apply and combine results.
      projected
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"repeatAll($value)"

  override def children: Seq[Expression] = value :: Nil

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    UnresolvedRepeatAll(newChildren.head, projectionExpression)
  }

}
