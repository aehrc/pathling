package au.csiro.pathling.sql.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{DataTypes, Decimal, DecimalType, ObjectType}

/**
 * Helper class for serialization of FlexiDecimals. 
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
    // scale -> the actual position of the decimal coma

    // TODO: Performance: consider if the normalized value could be cached in 
    // a variable 
    val normalizedExpression: Expression = StaticInvoke(classOf[FlexiDecimal],
      ObjectType(classOf[java.math.BigDecimal]),
      "normalize",
      expression :: Nil)

    val valueExpression: Expression = StaticInvoke(classOf[Decimal],
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
