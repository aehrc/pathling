/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.terminology.ucum.Ucum
import au.csiro.pathling.sql.types.FlexiDecimal
import au.csiro.pathling.sql.types.FlexiDecimalSupport.createFlexiDecimalSerializer
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{DataTypes, Decimal, DecimalType, ObjectType, StructField}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper class for serialization of Quantities. In order to improve the performance of
 * comparisons we store with each quantity canonicalized value (its value in the base unit of the quantity)
 * and the base (canonicalized) unit code.
 */
object QuantitySupport {

  val VALUE_CANONICALIZED_FIELD_NAME = "_value_canonicalized"
  val CODE_CANONICALIZED_FIELD_NAME = "_code_canonicalized"
  /**
   * Creates additional serializers for serialization of quantities.
   *
   * @param expression the Quantity expression to serialize
   * @return the serializers for canonicalized fields.
   */
  def createExtraSerializers(expression: Expression): Seq[(String, Expression)] = {
    val valueExp = Invoke(expression, "getValue", ObjectType(classOf[java.math.BigDecimal]))
    val codeExp = Invoke(expression, "getCode", ObjectType(classOf[java.lang.String]))

    val canonicalizedValue =
      createFlexiDecimalSerializer(
        StaticInvoke(classOf[Ucum], ObjectType(classOf[java.math.BigDecimal]),
          "getCanonicalValue", Seq(valueExp, codeExp)))

    val canonicalizedCode =
      StaticInvoke(
        classOf[UTF8String],
        DataTypes.StringType,
        "fromString",
        StaticInvoke(classOf[Ucum], ObjectType(classOf[java.lang.String]), "getCanonicalCode",
          Seq(valueExp, codeExp)) :: Nil)
    Seq(
      (VALUE_CANONICALIZED_FIELD_NAME, canonicalizedValue),
      (CODE_CANONICALIZED_FIELD_NAME, canonicalizedCode)
    )
  }

  /**
   * Creates additional schema fields for quantities.
   *
   * @return the schema of canonicalized fields.
   */
  def createExtraSchemaFields(): Seq[StructField] = {
    Seq(
      StructField(VALUE_CANONICALIZED_FIELD_NAME, FlexiDecimal.DATA_TYPE),
      StructField(CODE_CANONICALIZED_FIELD_NAME, DataTypes.StringType)
    )
  }
}
