package au.csiro.pathling.encoders2

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object EncoderUtils {

  def defaultResolveAndBind[T](expressionEncoder:ExpressionEncoder[T]):ExpressionEncoder[T]  = {
    expressionEncoder.resolveAndBind()
  }
}
