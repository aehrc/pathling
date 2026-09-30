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

import au.csiro.pathling.utilities.CanonicalStructure
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import au.csiro.pathling.encoders.UnevaluableCopy
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.types.DataType

import scala.util.{Failure, Success, Try}

/**
 * A combination of several operands of the same FHIR type, each projected by name into the
 * recursive field-wise merge of all of their types (FR-056).
 *
 * Each operand's projection depends on the types of all of the operands, which are known only once
 * Spark has resolved them (decision 75). A [[MergeCast]] for each operand must therefore hold every
 * operand, and a combination of such projections holds every operand once for each of them. Where
 * the combination is itself an operand of another, as in a chain of unions, the whole of the chain
 * to the left is held again at every level, and the plan doubles with every operand added.
 *
 * This expression holds each operand once, with the combination to apply to their projections.
 * Once every operand is resolved, it replaces itself with the combination of the projections, each
 * of which holds only its own operand, so that the plan grows linearly. An operand whose type is
 * already the merged type is its own projection, so where the operands already agree nothing is
 * added to the plan at all.
 *
 * Where the types of the operands cannot be merged, the projections are left to [[MergeCast]],
 * whose type check reports the failure as the analysis of any other combination does.
 *
 * @param operands    the full, ordered list of operands being combined
 * @param canonical   the canonical structure of the operands' element type
 * @param combination the combination of the projected operands, in the same order
 */
case class UnresolvedMergeCombination(operands: Seq[Expression], canonical: CanonicalStructure,
    combination: Seq[Expression] => Expression)
  extends Expression with UnevaluableCopy with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {
    val newOperands = operands.map(f)
    if (newOperands.forall(_.resolved)) {
      val projected = Try(MergeCast.mergedType(newOperands.map(_.dataType), canonical)) match {
        case Success(target) =>
          newOperands.map(operand => MergeCast.project(operand, operand.dataType, target))
        case Failure(_: IllegalArgumentException) =>
          newOperands.indices.map(index => MergeCast(newOperands, index, canonical))
        case Failure(e) => throw e
      }
      f(combination(projected))
    } else {
      copy(operands = newOperands)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"merge_combination(${operands.mkString(", ")})"

  override def children: Seq[Expression] = operands

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(operands = newChildren)
}
