/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticExpressionPathEncoder, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
object EncoderBuilder {

  val UNSUPPORTED_RESOURCES: Set[String] = Set("Parameters", "StructureDefinition", "StructureMap",
    "Bundle")

  /**
   * Custom AgnosticEncoder that wraps serializer and deserializer expressions.
   *
   * Note: This extends AgnosticExpressionPathEncoder which is deprecated and will be removed in
   * Spark 4.1. This is the appropriate mechanism for custom encoders that build Expression trees
   * directly. When Spark 4.1 is released, we will need to evaluate the replacement API.
   *
   * The deprecation warning is suppressed because:
   * 1. This is the correct API for our use case (custom expression-based encoding)
   * 2. The alternative (UserDefinedType) would require major architectural changes
   * 3. Spark may provide a better alternative in 4.1
   */
  @annotation.nowarn("cat=deprecation")
  private class FhirAgnosticEncoder[T](
                                        override val dataType: DataType,
                                        serializerExpr: Expression,
                                        deserializerExpr: Expression,
                                        override val clsTag: ClassTag[T]
                                      ) extends AgnosticExpressionPathEncoder[T] {
    override def isPrimitive: Boolean = false
    override def toCatalyst(input: Expression): Expression = serializerExpr
    override def fromCatalyst(inputPath: Expression): Expression = deserializerExpr
  }

  /**
   * Returns an encoder for the FHIR resource implemented by the given class
   *
   * @param resourceDefinition the FHIR resource definition
   * @param fhirContext        the FHIR context to use
   * @param mappings           the data type mappings to use
   * @param maxNestingLevel    the max nesting level to use to expand recursive data types.
   *                           Zero means that fields of type T are skipped in a composite od type T.
   * @param enableExtensions   true if support for extensions should be enabled.
   * @param openTypes          the list of types that are encoded within open types, such as extensions.
   * @return an ExpressionEncoder for the resource
   */
  def of(resourceDefinition: RuntimeResourceDefinition,
         fhirContext: FhirContext,
         mappings: DataTypeMappings,
         maxNestingLevel: Int,
         openTypes: Set[String],
         enableExtensions: Boolean): ExpressionEncoder[_] = {

    if (UNSUPPORTED_RESOURCES.contains(resourceDefinition.getName)) {
      throw new UnsupportedResourceError(
        s"Encoding is not supported for resource: ${resourceDefinition.getName}")
    }

    val fhirClass = resourceDefinition
      .asInstanceOf[BaseRuntimeElementDefinition[_]].getImplementingClass
    val schemaConverter = new SchemaConverter(fhirContext, mappings,
      EncoderConfig(maxNestingLevel, openTypes, enableExtensions))
    val serializerBuilder = SerializerBuilder(schemaConverter)
    val deserializerBuilder = DeserializerBuilder(schemaConverter)

    val serializerExpr = serializerBuilder.buildSerializer(resourceDefinition)
    val deserializerExpr = deserializerBuilder.buildDeserializer(resourceDefinition)
    val schema = serializerExpr.dataType

    val agnosticEncoder = new FhirAgnosticEncoder[Any](schema, serializerExpr, deserializerExpr,
      ClassTag(fhirClass))
    ExpressionEncoder(agnosticEncoder)
  }
}
