package au.csiro.pathling.encoders.datatypes

import ca.uhn.fhir.context.BaseRuntimeChildDefinition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructField

trait CustomCoder {

  def schema: Seq[StructField]
  def customDeserializer(addToPath: String => Expression): Map[String, Expression]
  def customSerializer(inputObject: Expression): List[Expression]

}
