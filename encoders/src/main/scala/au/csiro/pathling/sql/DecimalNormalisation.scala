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
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Cast, CaseWhen, EqualTo, Expression, GetArrayItem, GetStructField, LambdaFunction, LessThanOrEqual, Literal, NamedLambdaVariable, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._

/**
 * The normalisation of the previous layout's decimals to the new layout's text (T094b).
 *
 * The previous layout stores a decimal as a `DECIMAL(32,6)` value, with the scale of the source in
 * an integer companion whose name is the element's with a `_scale` suffix. A repeating decimal is
 * an array of values beside an array of scales. The new layout stores a decimal as text. So a
 * previous-layout decimal is normalised to the text of its value at the source scale, and the
 * engine above the traversal sees text on both layouts.
 *
 * The source scale is capped at the scale of the value column, because the digits beyond it were
 * rounded away on encoding, and a negative scale gives an integer. A value with no scale, which is
 * what the engine's own arithmetic produces, is rendered at the scale of the value column. The text
 * is a decimal without an exponent, and it parses back to the stored value exactly.
 *
 * Which branch applies is decided from the resolved schema, once per schema: a field is normalised
 * where its type is a decimal, or an array of decimals, and its scale is taken from the companion
 * where the structure carries one. Only the value is read per row.
 */
object DecimalNormalisation {

  /** The suffix of the name of the companion holding a decimal's source scale. */
  val SCALE_SUFFIX = "_scale"

  /**
   * Returns the name of the companion that holds the scale of a decimal element.
   *
   * @param fieldName the name of the decimal element
   * @return the name of its scale companion
   */
  def scaleFieldName(fieldName: String): String = fieldName + SCALE_SUFFIX

  /**
   * Returns true where a type is a decimal, or an array of decimals at any depth.
   *
   * @param dataType the type to inspect
   * @return true if the type holds decimals
   */
  def holdsDecimals(dataType: DataType): Boolean = dataType match {
    case _: DecimalType => true
    case ArrayType(elementType, _) => holdsDecimals(elementType)
    case _ => false
  }

  /**
   * Normalises a decimal field of a resolved structure to text, taking the scale from the
   * structure's companion where it has one.
   *
   * @param struct    the resolved structure
   * @param structType the type of the structure
   * @param fieldName the name of the decimal field
   * @return the text of the field
   */
  def fieldToText(struct: Expression, structType: StructType, fieldName: String): Expression = {
    val value = GetStructField(struct, structType.fieldIndex(fieldName), Some(fieldName))
    val scaleName = scaleFieldName(fieldName)
    val scale = if (structType.fieldNames.contains(scaleName)) {
      GetStructField(struct, structType.fieldIndex(scaleName), Some(scaleName))
    } else {
      Literal(null, NullType)
    }
    toText(value, scale)
  }

  /**
   * Normalises a decimal, or an array of decimals, to text at the given scale, or the parallel
   * array of scales. Where the value is not a decimal, it is returned unchanged.
   *
   * @param value the decimal, or array of decimals
   * @param scale the scale, the parallel array of scales, or a null where there is none
   * @return the text of the value
   */
  def toText(value: Expression, scale: Expression): Expression = value.dataType match {
    case decimalType: DecimalType =>
      atScale(value, decimalType, integerScale(scale))
    case ArrayType(elementType, containsNull) if holdsDecimals(elementType) =>
      val element = NamedLambdaVariable("value", elementType, containsNull)
      val index = NamedLambdaVariable("index", IntegerType, nullable = false)
      val elementScale = scale.dataType match {
        case _: ArrayType => GetArrayItem(scale, index, failOnError = false)
        case _ => Literal(null, IntegerType)
      }
      ArrayTransform(value, LambdaFunction(toText(element, elementScale), Seq(element, index)))
    case _ =>
      value
  }

  /**
   * Renders a decimal as text at a scale from zero to the scale of its type. Each scale below that
   * of the type is a cast to a decimal of that scale, whose precision leaves room for the carry of
   * rounding, so that no cast can overflow. A scale at or above that of the type, or a null scale,
   * renders the value as it is stored.
   */
  private def atScale(value: Expression, decimalType: DecimalType,
                      scale: Expression): Expression = {
    val branches = (0 until decimalType.scale).map { target =>
      val condition = if (target == 0) {
        LessThanOrEqual(scale, Literal(0))
      } else {
        EqualTo(scale, Literal(target))
      }
      val precision = math.min(DecimalType.MAX_PRECISION,
        decimalType.precision - decimalType.scale + target + 1)
      condition -> asText(Cast(value, DecimalType(precision, target)))
    }
    if (branches.isEmpty) {
      asText(value)
    } else {
      CaseWhen(branches, asText(value))
    }
  }

  private def asText(value: Expression): Expression = Cast(value, StringType)

  private def integerScale(scale: Expression): Expression = scale.dataType match {
    case IntegerType => scale
    case ByteType | ShortType | LongType => Cast(scale, IntegerType)
    case _ => Literal(null, IntegerType)
  }

  /**
   * Creates a tolerant reference to a table-level decimal column, normalised to text. At the root
   * there is no parent structure to inspect, so the value and its scale companion are both reached
   * through the tolerant table-column reference (decision 75), and [[NormaliseDecimal]] decides from
   * their resolved types.
   *
   * @param columnName the name of the table-level column
   * @param fallback   the type of the null returned when the column is absent
   * @return the expression
   */
  def tolerantColumn(columnName: String, fallback: DataType): Expression =
    NormaliseDecimal(UnresolvedColumnOrNull(columnName, fallback),
      UnresolvedColumnOrNull(scaleFieldName(columnName), NullType))
}

/**
 * Normalises a resolved decimal, or array of decimals, to text at the scale given beside it, and
 * passes any other value through unchanged. See [[DecimalNormalisation]].
 *
 * This is the form the resource root needs, where the value and its scale are table columns rather
 * than fields of a structure the traversal expression can inspect. It follows the construction of
 * [[ResolveOrNull]]: the replacement is a `lazy val` built from resolved children only.
 *
 * @param left  the value
 * @param right the scale, the parallel array of scales, or a null where there is none
 */
case class NormaliseDecimal(left: Expression, right: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {

  override lazy val replacement: Expression = DecimalNormalisation.toText(left, right)

  override def prettyName: String = "normalise_decimal"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}
