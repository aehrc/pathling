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

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, CreateNamedStruct, Expression,
  IsNotNull, Or, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.BinaryLike

/**
 * The accessor of an annotation of an engine element: the annotation field where the element's
 * resolved type carries it, and the fallback, which computes the annotation from the element's
 * value, where it does not (design §4.3). This is [[ResolveOrElse]], with one addition.
 *
 * A singular element is built as `CASE WHEN isnotnull(value) THEN struct(value, ...) END`, so
 * that an absent value is an absent element. Spark reads a field through that `CASE` as a `CASE`
 * of the field, which hides the stored field from Parquet's filter pushdown
 * (`evidence/annotations-design-pushdown.md`, arm 4). An annotation is null wherever its value is,
 * because the annotation step computes it from the value and the engine takes a stored annotation
 * as given (ticket 31), so the guard adds nothing to it. Where the element is such a guarded
 * structure and carries the field, the accessor is therefore the field's own expression, a plain
 * read of the stored annotation that Parquet can push down. Only guards that test the element's
 * own `value`, or a constant that is present, are seen through, as traversal builds them at each
 * level it descends through. A primitive read with its id and extensions is an element where any
 * of them is present (decision 154), so its guard is a disjunction of such tests; it is seen
 * through where one of its tests is of the value, because the guard is then false only where the
 * value is absent too. Any other guard, such as the criteria of a `where()`, can be false where
 * the value is present, so an element under one is read through it as [[ResolveOrElse]] reads it.
 *
 * @param child     the element
 * @param fieldName the name of the annotation field
 * @param fallback  the annotation computed from the element's value, taken where the field is
 *                  absent
 */
case class AnnotationOrElse(child: Expression, fieldName: String, fallback: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {

  override def left: Expression = child

  override def right: Expression = fallback

  override lazy val replacement: Expression = structOf(child, Seq.empty)
    .filter(_.names.map(String.valueOf).contains(fieldName))
    .map(struct => struct.valExprs(struct.names.map(String.valueOf).indexOf(fieldName)))
    .getOrElse(ResolveOrElse(child, fieldName, fallback).replacement)

  /**
   * The structure a guarded element builds, where it is built in the plan under guards that are
   * each false only where the element's value is absent. Each guard is given as the expressions
   * its disjuncts test.
   */
  private def structOf(element: Expression,
                       guards: Seq[Seq[Expression]]): Option[CreateNamedStruct] =
    element match {
      case CaseWhen(Seq((condition, value)), None) if testsOf(condition).isDefined =>
        structOf(value, guards :+ testsOf(condition).get)
      case struct: CreateNamedStruct
        if guards.forall(_.exists(tested =>
          present(tested) || valueOf(struct).exists(tested.semanticEquals))) =>
        Some(struct)
      case resolved: ResolveOrNone if !resolved.repeating => structOf(resolved.replacement, guards)
      case _ => None
    }

  /**
   * The expressions a guard tests, where it is a test that one expression is present or a
   * disjunction of such tests.
   */
  private def testsOf(condition: Expression): Option[Seq[Expression]] = condition match {
    case IsNotNull(tested) => Some(Seq(tested))
    case Or(left, right) => for {
      l <- testsOf(left)
      r <- testsOf(right)
    } yield l ++ r
    case _ => None
  }

  /** Whether a guard's tested expression is a constant that is present. */
  private def present(tested: Expression): Boolean = tested.foldable && tested.eval() != null

  /** The element's own value, the structure's `value` field. */
  private def valueOf(struct: CreateNamedStruct): Option[Expression] =
    struct.names.map(String.valueOf).indexOf(AnnotationOrElse.ValueField) match {
      case -1 => None
      case ordinal => Some(struct.valExprs(ordinal))
    }

  override def prettyName: String = "annotation_or_else"

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): Expression =
    copy(child = newLeft, fallback = newRight)
}

object AnnotationOrElse {

  /** The name of an engine element's value (decision 152). */
  private val ValueField = "value"
}
