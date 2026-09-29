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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.DataType

import scala.util.{Failure, Success, Try}

/**
 * The reconciliation of several operands at once: projects every one of several operands of the
 * same FHIR type by name into the recursive field-wise merge of all of their types, and holds the
 * projections as the fields of one structure (FR-056).
 *
 * The projection is that of [[MergeCast]], which projects one operand and so must be given every
 * operand to compute the merged type. A site that needs the projections of all of the operands
 * would take one [[MergeCast]] for each, and so hold every operand once for each of them. Where the
 * result of the site is an operand of another such site, as in a chain of unions, the whole of the
 * chain to the left would then be held again at every level, and the plan would double with every
 * operand added. This expression holds each operand once, and the site reads the projections from
 * its fields through one binding of the structure.
 *
 * The field that holds the projection of an operand is named by [[MergeCastAll.fieldName]].
 *
 * @param operands  the full, ordered list of operands being reconciled
 * @param canonical the canonical structure of the operands' element type
 */
case class MergeCastAll(operands: Seq[Expression], canonical: CanonicalStructure)
  extends RuntimeReplaceable {

  /** The type every operand is projected into. */
  lazy val targetType: DataType = MergeCast.mergedType(operands.map(_.dataType), canonical)

  override lazy val replacement: Expression = CreateNamedStruct(
    operands.zipWithIndex.flatMap { case (operand, index) =>
      Seq(Literal(MergeCastAll.fieldName(index)),
        MergeCast.project(operand, operand.dataType, targetType))
    })

  override def checkInputDataTypes(): TypeCheckResult =
    Try(targetType) match {
      case Success(_) => TypeCheckSuccess
      case Failure(e: IllegalArgumentException) => TypeCheckFailure(e.getMessage)
      case Failure(e) => throw e
    }

  override def children: Seq[Expression] = operands

  override def prettyName: String = "merge_cast_all"

  override def flatArguments: Iterator[Any] = Iterator(operands)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(operands = newChildren)
}

object MergeCastAll {

  /**
   * Returns the name of the field that holds the projection of an operand.
   *
   * @param index the position of the operand in the list
   * @return the name of the field
   */
  def fieldName(index: Int): String = "_" + index
}
