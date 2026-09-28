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

import au.csiro.pathling.encoders.{QuantitySupport, UnresolvedColumnOrNull}
import au.csiro.pathling.sql.DecimalNormalisation.{fieldToText, holdsDecimals, scaleFieldName}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, CreateNamedStruct, Expression, GetStructField, If, IsNull, LambdaFunction, Literal, NamedLambdaVariable, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
 * The normalisation of the previous layout's quantities to the new layout's shape (T094b).
 *
 * The previous layout stores a quantity with its canonical form beside it, in the
 * `_value_canonicalized` and `_code_canonicalized` fields, and with the scale of its value in a
 * `value_scale` companion. The new layout stores neither: until annotations are emitted, the
 * canonical form of a quantity is computed from the quantity itself (FR-022). So a previous-layout
 * quantity is normalised to a structure without its canonical form, whose value is its text at the
 * source scale, as [[DecimalNormalisation]] renders it, and the engine above the traversal computes
 * the canonical form on both layouts.
 *
 * The other fields are kept in their stored order. That includes `_fid`, which the next step needs
 * to look up the extensions of the quantity, so a normalised quantity keeps one field that the new
 * layout does not have, as the extension structure does (decisions 74 and 75). Every leaf reached
 * through it has the new layout's type.
 *
 * Which branch applies is decided from the resolved schema, once per schema: a structure is a
 * previous-layout quantity where it carries both canonical fields, and nothing else is changed.
 */
object QuantityNormalisation {

  /** The field of a previous-layout quantity that holds its canonical value. */
  val CANONICAL_VALUE_FIELD: String = QuantitySupport.VALUE_CANONICALIZED_FIELD_NAME

  /** The field of a previous-layout quantity that holds its canonical unit code. */
  val CANONICAL_CODE_FIELD: String = QuantitySupport.CODE_CANONICALIZED_FIELD_NAME

  /**
   * Returns true where a structure is a previous-layout quantity, which carries its canonical form.
   *
   * @param struct the type of the structure
   * @return true if the structure carries both canonical fields
   */
  def isPreviousLayoutQuantity(struct: StructType): Boolean =
    struct.fieldNames.contains(CANONICAL_VALUE_FIELD) &&
      struct.fieldNames.contains(CANONICAL_CODE_FIELD)

  /**
   * Returns true where a type is a previous-layout quantity, or an array of them at any depth.
   *
   * @param dataType the type to inspect
   * @return true if the type holds previous-layout quantities
   */
  def holdsQuantities(dataType: DataType): Boolean = dataType match {
    case struct: StructType => isPreviousLayoutQuantity(struct)
    case ArrayType(elementType, _) => holdsQuantities(elementType)
    case _ => false
  }

  /**
   * Normalises a previous-layout quantity, or an array of them, to the new layout's shape. Any
   * other value is returned unchanged.
   *
   * @param value the resolved value
   * @return the value, with every previous-layout quantity it holds normalised
   */
  def normalise(value: Expression): Expression = value.dataType match {
    case struct: StructType if isPreviousLayoutQuantity(struct) =>
      toNewLayout(value, struct)
    case ArrayType(elementType, containsNull) if holdsQuantities(elementType) =>
      val element = NamedLambdaVariable("quantity", elementType, containsNull)
      ArrayTransform(value, LambdaFunction(normalise(element), Seq(element)))
    case _ =>
      value
  }

  private def toNewLayout(quantity: Expression, struct: StructType): Expression = {
    val decimals = struct.fields.filter(field => holdsDecimals(field.dataType)).map(_.name)
    val dropped = Set(CANONICAL_VALUE_FIELD, CANONICAL_CODE_FIELD) ++ decimals.map(scaleFieldName)
    val fields = struct.fields.toSeq.filterNot(field => dropped.contains(field.name)).flatMap {
      field =>
        val value = if (decimals.contains(field.name)) {
          fieldToText(quantity, struct, field.name)
        } else {
          GetStructField(quantity, struct.fieldIndex(field.name), Some(field.name))
        }
        Seq(Literal(field.name), value)
    }
    val normalised = CreateNamedStruct(fields)
    // A null quantity stays null, rather than becoming a structure of nulls.
    If(IsNull(quantity), Literal(null, normalised.dataType), normalised)
  }

  /**
   * Creates a tolerant reference to a table-level quantity column, normalised to the new layout's
   * shape. At the root there is no parent structure to inspect, so the column is reached through
   * the tolerant table-column reference (decision 75), and [[NormaliseQuantity]] decides from its
   * resolved type.
   *
   * @param columnName the name of the table-level column
   * @param fallback   the type of the null returned when the column is absent
   * @return the expression
   */
  def tolerantColumn(columnName: String, fallback: DataType): Expression =
    NormaliseQuantity(UnresolvedColumnOrNull(columnName, fallback))
}

/**
 * Normalises a resolved previous-layout quantity, or array of them, to the new layout's shape, and
 * passes any other value through unchanged. See [[QuantityNormalisation]].
 *
 * This is the form the resource root needs, where the quantity is a table column rather than a
 * field of a structure the traversal expression can inspect. It follows the construction of
 * [[ResolveOrNull]]: the replacement is a `lazy val` built from resolved children only.
 *
 * @param child the value
 */
case class NormaliseQuantity(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = QuantityNormalisation.normalise(child)

  override def prettyName: String = "normalise_quantity"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
