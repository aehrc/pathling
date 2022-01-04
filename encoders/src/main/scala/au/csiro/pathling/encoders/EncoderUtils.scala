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
