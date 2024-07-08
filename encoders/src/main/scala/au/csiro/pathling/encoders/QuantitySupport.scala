/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.terminology.ucum.Ucum
import au.csiro.pathling.sql.types.FlexiDecimal
import au.csiro.pathling.sql.types.FlexiDecimalSupport.createFlexiDecimalSerializer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{DataTypes, ObjectType, StructField}
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
        Catalyst.staticInvoke(classOf[Ucum], ObjectType(classOf[java.math.BigDecimal]),
          "getCanonicalValue", Seq(valueExp, codeExp)))

    val canonicalizedCode =
      Catalyst.staticInvoke(
        classOf[UTF8String],
        DataTypes.StringType,
        "fromString",
        Catalyst.staticInvoke(classOf[Ucum], ObjectType(classOf[java.lang.String]), "getCanonicalCode",
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
