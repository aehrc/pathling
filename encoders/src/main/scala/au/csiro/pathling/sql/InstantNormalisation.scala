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

import au.csiro.pathling.encoders.UnresolvedColumnOrNull
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Concat, DateFormatClass, Expression, LambdaFunction, Literal, NamedLambdaVariable, RegExpReplace, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType, TimestampType}

/**
 * The normalisation of the previous layout's instants to the new layout's text (decision 81).
 *
 * The previous layout stores an `instant` as a timestamp, which keeps the point in time but not
 * the offset it was given with. The new layout stores it as text, in its lexical form. So a
 * previous-layout instant is normalised to text, and the engine above the traversal sees text on
 * both layouts. The offset cannot be recovered, so the text is the point in time in UTC, in
 * ISO 8601 form with a `Z` suffix: `2023-01-01T02:00:00Z`. Fractional seconds are included only
 * where they are not zero, without trailing zeros: `2023-01-01T02:00:00.123Z`. The text does not
 * depend on the session time zone.
 *
 * The previous layout stores no other FHIR type as a timestamp, so a timestamp-typed field is an
 * instant. Which branch applies is decided from the resolved schema, once per schema: a value is
 * normalised where its type is a timestamp, or an array of timestamps at any depth, and passed
 * through unchanged otherwise.
 */
object InstantNormalisation {

  /** The pattern of the text, with every fractional digit that a timestamp can hold. */
  private val PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

  /** Matches the zeros that end the fraction, and the point too where the fraction is all zeros. */
  private val TRAILING_ZEROS = "\\.?0*$"

  /** The time zone the text is rendered in, whatever the session's. */
  private val UTC = "UTC"

  /**
   * Returns true where a type is a timestamp, or an array of timestamps at any depth.
   *
   * @param dataType the type to inspect
   * @return true if the type holds timestamps
   */
  def holdsInstants(dataType: DataType): Boolean = dataType match {
    case TimestampType => true
    case ArrayType(elementType, _) => holdsInstants(elementType)
    case _ => false
  }

  /**
   * Normalises a timestamp, or an array of timestamps, to its UTC text. Where the value holds no
   * timestamps, it is returned unchanged.
   *
   * @param value the value
   * @return the text of the value, or the value itself
   */
  def normalise(value: Expression): Expression = value.dataType match {
    case TimestampType =>
      val text = DateFormatClass(value, Literal(PATTERN), Some(UTC))
      Concat(Seq(RegExpReplace(text, Literal(TRAILING_ZEROS), Literal("")), Literal("Z")))
    case ArrayType(elementType, containsNull) if holdsInstants(elementType) =>
      val element = NamedLambdaVariable("instant", elementType, containsNull)
      ArrayTransform(value, LambdaFunction(normalise(element), Seq(element)))
    case _ =>
      value
  }

  /**
   * Creates a tolerant reference to a table-level instant column, normalised to text. At the root
   * there is no parent structure to inspect, so the column is reached through the tolerant
   * table-column reference (decision 75), and [[NormaliseInstant]] decides from its resolved type.
   *
   * @param columnName the name of the table-level column
   * @param fallback   the type of the null returned when the column is absent
   * @return the expression
   */
  def tolerantColumn(columnName: String, fallback: DataType): Expression =
    NormaliseInstant(UnresolvedColumnOrNull(columnName, fallback))
}

/**
 * Normalises a resolved previous-layout instant, or array of them, to its UTC text, and passes any
 * other value through unchanged. See [[InstantNormalisation]].
 *
 * This is the form the resource root needs, where the instant is a table column rather than a
 * field of a structure the traversal expression can inspect. It follows the construction of
 * [[ResolveOrNull]]: the replacement is a `lazy val` built from resolved children only.
 *
 * @param child the value
 */
case class NormaliseInstant(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = InstantNormalisation.normalise(child)

  override def prettyName: String = "normalise_instant"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
