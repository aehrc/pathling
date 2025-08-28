/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.sql.types

import au.csiro.pathling.encoders.Catalyst
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{DataTypes, Decimal, ObjectType}

/**
 * Helper class for serialization of FlexiDecimals.
 *
 * @author Piotr Szul
 */
object FlexiDecimalSupport {

  def toLiteral(value: java.math.BigDecimal): Column = {
    val normalizedValue = FlexiDecimal.normalize(value)
    if (normalizedValue != null) {
      struct(
        lit(Decimal.apply(normalizedValue.unscaledValue())).cast(FlexiDecimal.DECIMAL_TYPE)
          .as("value"),
        lit(normalizedValue.scale()).as("scale")
      )
    } else {
      lit(null)
    }
  }

  def createFlexiDecimalSerializer(expression: Expression): Expression = {
    // the expression is of type BigDecimal
    // we want to encode it as a struct of two fields
    // value -> the BigInteger representing digits
    // scale -> the actual position of the decimal point

    // TODO: Performance: consider if the normalized value could be cached in
    //   a variable
    val normalizedExpression: Expression = Catalyst.staticInvoke(classOf[FlexiDecimal],
      ObjectType(classOf[java.math.BigDecimal]),
      "normalize",
      expression :: Nil)

    val valueExpression: Expression = Catalyst.staticInvoke(classOf[Decimal],
      FlexiDecimal.DECIMAL_TYPE,
      "apply",
      Invoke(normalizedExpression, "unscaledValue",
        ObjectType(classOf[java.math.BigInteger])) :: Nil)

    val scaleExpression: Expression = Invoke(normalizedExpression, "scale", DataTypes.IntegerType)

    val struct = CreateNamedStruct(Seq(
      Literal("value"), valueExpression,
      Literal("scale"), scaleExpression
    ))
    If(IsNull(expression), Literal.create(null, struct.dataType), struct)
  }
}
