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

import au.csiro.pathling.utilities.{CanonicalStructure, StructureMerge}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, CreateNamedStruct, Expression, GetStructField, If, IsNull, KnownNotNull, KnownNullable, LambdaFunction, Literal, NamedLambdaVariable, RuntimeReplaceable}
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * The reconciliation expression: projects one of several operands of the same FHIR type **by
 * name** into the recursive field-wise merge of all of their types (FR-056).
 *
 * Under a fitted schema, two collections of the same FHIR type reached by different paths have
 * different SQL shapes, and every operation needing a common type fails on them. Each operand is
 * therefore replaced with its projection into the merged type, and the projections combine. Every
 * operand is given the full operand list, so that each computes the same target type.
 *
 * The merge is [[StructureMerge]], under the supplied canonical structure, which orders the fields
 * at every level (FR-057). The canonical structure is an input because the operands cannot supply
 * it: two subsequences of a total order do not determine that order. It is taken through the
 * interface declared in `utilities`, so that this module never sees the definitions (FR-051).
 *
 * The projection is by name and never a cast, because a cast between structures of equal arity
 * reorders their fields positionally and silently. A field an operand lacks becomes a null, and a
 * null structure stays null rather than becoming a structure of nulls. An operand of the bottom
 * type, or an array of it, is how an absent element is typed (FR-055): it takes part in no merge
 * and projects to a null of the merged type.
 *
 * The operands must be all structures or all arrays of structures, apart from those of the bottom
 * type, which combine with either. Operands of any other type must already agree, and are left as
 * they are.
 *
 * @param operands  the full, ordered list of operands being reconciled
 * @param index     the position in the list of the operand this expression projects
 * @param canonical the canonical structure of the operands' element type
 */
case class MergeCast(operands: Seq[Expression], index: Int, canonical: CanonicalStructure)
  extends RuntimeReplaceable {

  /** The type every operand is projected into. */
  lazy val targetType: DataType = MergeCast.mergedType(operands.map(_.dataType), canonical)

  override lazy val replacement: Expression = {
    val operand = operands(index)
    MergeCast.project(operand, operand.dataType, targetType)
  }

  override def checkInputDataTypes(): TypeCheckResult =
    Try(targetType) match {
      case Success(_) => TypeCheckSuccess
      case Failure(e: IllegalArgumentException) => TypeCheckFailure(e.getMessage)
      case Failure(e) => throw e
    }

  override def children: Seq[Expression] = operands

  override def prettyName: String = "merge_cast"

  override def flatArguments: Iterator[Any] = Iterator(operands, index)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(operands = newChildren)
}

object MergeCast {

  /**
   * Computes the type that operands of the given types are projected into.
   *
   * @param types     the types of every operand
   * @param canonical the canonical structure of the operands' element type
   * @return the merged type
   * @throws IllegalArgumentException if the types cannot be merged
   */
  def mergedType(types: Seq[DataType], canonical: CanonicalStructure): DataType = {
    val present = types.filterNot(isAbsent)
    if (present.isEmpty) {
      types.head
    } else {
      val repeating = types.exists(_.isInstanceOf[ArrayType])
      if (repeating && !present.forall(_.isInstanceOf[ArrayType])) {
        throw new IllegalArgumentException(
          "Cannot reconcile singular and repeating operands: " + types.map(_.simpleString)
            .mkString(", "))
      }
      val elements = present.map(elementType)
      val element = elements match {
        case structs if structs.forall(_.isInstanceOf[StructType]) =>
          StructureMerge.merge(structs.map(_.asInstanceOf[StructType]).asJava, canonical)
        case others if others.forall(_ == others.head) =>
          others.head
        case others =>
          throw new IllegalArgumentException(
            "Cannot reconcile operands of different types: " + others.map(_.simpleString)
              .mkString(", "))
      }
      if (repeating) {
        ArrayType(element, types.exists {
          case ArrayType(_, containsNull) => containsNull
          case _ => true
        })
      } else {
        element
      }
    }
  }

  /**
   * Projects a value by name into a target type, recursively.
   *
   * The result has exactly the target type, nullability included, so that every operand's
   * projection has the same type and nothing downstream needs to widen them.
   *
   * @param value the value to project
   * @param from  the value's type
   * @param to    the target type
   * @return the projected value
   */
  def project(value: Expression, from: DataType, to: DataType): Expression = (from, to) match {
    case (f, t) if f == t => value
    case (NullType, t) => Literal(null, t)
    case (ArrayType(NullType, _), ArrayType(t, containsNull)) =>
      val element = NamedLambdaVariable("element", NullType, nullable = true)
      ArrayTransform(value,
        LambdaFunction(withNullability(Literal(null, t), containsNull), Seq(element)))
    case (fs: StructType, ts: StructType) =>
      val struct = CreateNamedStruct(ts.fields.toSeq.flatMap { field =>
        val projected = fs.fieldNames.indexOf(field.name) match {
          case ordinal if ordinal >= 0 =>
            project(GetStructField(value, ordinal, Some(field.name)), fs(ordinal).dataType,
              field.dataType)
          case _ => Literal(null, field.dataType)
        }
        Seq(Literal(field.name), withNullability(projected, field.nullable))
      })
      if (value.nullable) If(IsNull(value), Literal(null, ts), struct) else struct
    case (ArrayType(fe, fromContainsNull), ArrayType(te, containsNull)) =>
      val element = NamedLambdaVariable("element", fe, fromContainsNull)
      ArrayTransform(value,
        LambdaFunction(withNullability(project(element, fe, te), containsNull), Seq(element)))
    case _ => value
  }

  /**
   * Declares a value's nullability to be what the target type says. Where the target type says a
   * value cannot be null, every operand's type said so too, and the value is inside a structure
   * already known not to be null, so the declaration is true.
   */
  private def withNullability(value: Expression, nullable: Boolean): Expression =
    if (value.nullable == nullable) {
      value
    } else if (nullable) {
      KnownNullable(value)
    } else {
      KnownNotNull(value)
    }

  private def isAbsent(dataType: DataType): Boolean = dataType match {
    case NullType | ArrayType(NullType, _) => true
    case _ => false
  }

  private def elementType(dataType: DataType): DataType = dataType match {
    case ArrayType(element, _) => element
    case other => other
  }
}
