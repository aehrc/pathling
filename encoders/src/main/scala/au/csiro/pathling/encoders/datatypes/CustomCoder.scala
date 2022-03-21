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

package au.csiro.pathling.encoders.datatypes

import au.csiro.pathling.encoders2.ExpressionWithName
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructField}

trait CustomCoder {
  def schema(arrayEncoder: Option[DataType => DataType]): Seq[StructField]

  def customSerializer(evaluator: (Expression => Expression) => Expression): Seq[ExpressionWithName]

  def customDeserializer(addToPath: String => Expression, isCollection: Boolean): Seq[ExpressionWithName]

}
