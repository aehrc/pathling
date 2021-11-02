/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders.datatypes

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder.decimalType
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.hl7.fhir.r4.model.DecimalType


/**
 * Custom coder for DecimalType.
 * Represents decimal on two dataset columns: the actual DecimalType value and
 * an additional IntegerType column with `_scale` suffix storing the scale of the
 * original FHIR decimal value.
 *
 * @param elementName the name of the element that this will be used to encode
 */
case class DecimalCustomCoder(elementName: String) extends CustomCoder {

  def primitiveClass: Class[DecimalType] = classOf[DecimalType]

  val scaleFieldName: String = elementName + "_scale"

  override val schema: Seq[StructField] = Seq(StructField(elementName, decimalType), StructField(scaleFieldName, IntegerType))

  override def customDecoderExpression(addToPath: String => Expression): Expression = {
    NewInstance(primitiveClass,
      Invoke(
        Invoke(addToPath(elementName), "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal])),
        "setScale", ObjectType(classOf[java.math.BigDecimal]), addToPath(scaleFieldName) :: Nil
      ) :: Nil,
      ObjectType(primitiveClass)
    )
  }

  override def customSerializer(inputObject: Expression): List[Expression] = {
    val valueExpression = StaticInvoke(classOf[Decimal],
      decimalType,
      "apply",
      Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])) :: Nil)
    val scaleExpression = StaticInvoke(classOf[Math],
      IntegerType,
      "min", Literal(decimalType.scale) ::
        Invoke(Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])),
          "scale", DataTypes.IntegerType) :: Nil
    )
    List(Literal(elementName), valueExpression, Literal(scaleFieldName), scaleExpression)
  }

  override def customSerializer2(inputObject: Expression): Seq[(String, Expression)] = {
    val valueExpression = StaticInvoke(classOf[Decimal],
      decimalType,
      "apply",
      Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])) :: Nil)
    val scaleExpression = StaticInvoke(classOf[Math],
      IntegerType,
      "min", Literal(decimalType.scale) ::
        Invoke(Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])),
          "scale", DataTypes.IntegerType) :: Nil
    )
    Seq((elementName, valueExpression), (scaleFieldName, scaleExpression))
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
   * So the final type is DECIMAL(26,6) which allows both for 6 decimal places and
   * at least 18 digits (regardless if there any decimal digits or not)
   */

  val scale: Int = 6
  val precision: Int = 26
  val decimalType: types.DecimalType = DataTypes.createDecimalType(precision, scale)
}
