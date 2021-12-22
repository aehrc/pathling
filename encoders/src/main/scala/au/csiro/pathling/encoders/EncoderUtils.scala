package au.csiro.pathling.encoders

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.ObjectType

import java.util

object EncoderUtils {

  def defaultResolveAndBind[T](expressionEncoder: ExpressionEncoder[T]): ExpressionEncoder[T] = {
    expressionEncoder.resolveAndBind()
  }

  def arrayExpression(array: Expression): StaticInvoke = {
    StaticInvoke(
      classOf[util.Arrays],
      ObjectType(classOf[util.List[_]]),
      "asList",
      array :: Nil)
  }
  
}
