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
package au.csiro.pathling.encoders.datatypes

import au.csiro.pathling.encoders.EncoderUtils.arrayExpression
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder.decimalType
import au.csiro.pathling.encoders.{Catalyst, ExpressionWithName}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.hl7.fhir.r4.model.DecimalType

/**
 * Custom coder for DecimalType.
 * Represents decimal on two dataset columns: the actual DecimalType value and
 * an additional IntegerType column with `_scale` suffix storing the scale of the
 * original FHIR decimal value.
 *
 * The scale is written without any cap, so a stored scale greater than the scale of the value
 * column identifies a value that was rounded during encoding. The cap is applied when reading,
 * where the scale set on the reconstructed value is the lesser of the stored scale and the scale
 * of the value column.
 *
 * @param elementName the name of the element that this will be used to encode
 */
case class DecimalCustomCoder(elementName: String) extends CustomCoder {

  def primitiveClass: Class[DecimalType] = classOf[DecimalType]

  val scaleFieldName: String = elementName + "_scale"

  override def customDeserializer(addToPath: String => Expression,
                                  isCollection: Boolean): Seq[ExpressionWithName] = {

    val deserializer = if (!isCollection) {
      decimalExpression(addToPath)
    } else {
      // Let's to this manually as we need to zip two independent arrays into into one
      val array = Catalyst.staticInvoke(
        classOf[DecimalCustomCoder],
        ObjectType(classOf[Array[Any]]),
        "zipToDecimal",
        addToPath(elementName) :: addToPath(scaleFieldName) :: Nil
      )
      arrayExpression(array)
    }
    Seq((elementName, deserializer))
  }

  override def customSerializer(evaluator: (Expression => Expression) => Expression): Seq[ExpressionWithName] = {
    val valueExpression = evaluator(exp => Catalyst.staticInvoke(classOf[Decimal],
      decimalType,
      "apply",
      Invoke(exp, "getValue", ObjectType(classOf[java.math.BigDecimal])) :: Nil))
    val scale = evaluator(exp => scaleExpression(exp))
    Seq((elementName, valueExpression), (scaleFieldName, scale))
  }

  override def schema(arrayEncoder: Option[DataType => DataType]): Seq[StructField] = {
    def encode(v: DataType): DataType = {
      arrayEncoder.map(_(v)).getOrElse(v)
    }

    Seq(StructField(elementName, encode(decimalType)),
      StructField(scaleFieldName, encode(IntegerType)))
  }

  /**
   * Decodes the value column into a DecimalType, setting the scale to the lesser of the stored
   * scale and the scale of the value column. The stored scale can exceed the scale of the value
   * column, in which case the additional digits are no longer available and must not be
   * reinstated.
   */
  private def decimalExpression(addToPath: String => Expression) = {
    val clampedScale = Catalyst.staticInvoke(classOf[Math],
      IntegerType,
      "min", Literal(decimalType.scale) :: addToPath(scaleFieldName) :: Nil
    )
    NewInstance(primitiveClass,
      Invoke(
        Invoke(addToPath(elementName), "toJavaBigDecimal",
          ObjectType(classOf[java.math.BigDecimal])),
        "setScale", ObjectType(classOf[java.math.BigDecimal]), clampedScale :: Nil
      ) :: Nil,
      ObjectType(primitiveClass)
    )
  }

  /**
   * Encodes the scale of the original FHIR decimal value, without any cap, so that a stored scale
   * greater than the scale of the value column identifies a value that lost precision during
   * encoding.
   */
  private def scaleExpression(inputObject: Expression) = {
    Invoke(Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])),
      "scale", DataTypes.IntegerType)
  }

}

object DecimalCustomCoder {

  /**
   * SQL DecimalType used to represent FHIR Decimal
   *
   * The specs says it should follow the rules for xs:decimal from:
   * https://www.w3.org/TR/xmlschema-2/, which are the
   * "All ·minimally conforming· processors ·must· support decimal numbers with a minimum of 18 decimal digits
   * (i.e., with a ·totalDigits· of 18). However, ·minimally conforming· processors ·may· set an
   * application-defined limit on the maximum number of decimal digits they are prepared to support,
   * in which case that application-defined maximum number ·must· be clearly documented."
   *
   * On the other hand FHIR spec for decimal reads:
   * "Note that large and/or highly precise values are extremely rare in medicine.
   * One element where highly precise decimals may be encountered is the Location coordinates.
   * Irrespective of this, the limits documented in XML Schema apply"
   *
   * For location coordinates 6 decimal digits allow for location precision of 10cm,
   * so should be sufficient for any medical purpose.
   *
   * So the final type is DECIMAL(32,6) which allows both for 6 decimal places and 26 digits
   * (regardless if there any decimal digits or not)
   */

  val scale: Int = 6
  val precision: Int = 32
  val decimalType: types.DecimalType = DataTypes.createDecimalType(precision, scale)


  /**
   * Zips the value and scale arrays so that they can be decoded to an array of DecimalType.
   *
   * The scale applied to each element is the lesser of the stored scale and the scale of the value
   * column, as the stored scale records the scale of the original FHIR decimal value and can
   * exceed what the value column represents.
   */
  def zipToDecimal(values: ArrayData, scales: ArrayData): Array[DecimalType] = {
    assert(values.numElements() == scales.numElements(),
      "Values and scales must have the same length")
    Array.tabulate(values.numElements()) { i =>
      new DecimalType(
        values.getDecimal(i, precision, scale).toJavaBigDecimal
          .setScale(Math.min(scales.getInt(i), scale)))
    }
  }
}
