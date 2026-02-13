/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
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
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}
import org.apache.spark.sql.types.DataType

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
object EncoderBuilder {

  // Delegate to the central ResourceTypes utility class for the set of unsupported resources.
  val UNSUPPORTED_RESOURCES: Set[String] = ResourceTypes.UNSUPPORTED_RESOURCES.asScala.toSet

  /**
   * Metadata-only AgnosticEncoder that provides the data type and class tag for FHIR resources.
   * The serialiser and deserialiser expressions are passed directly to the ExpressionEncoder
   * constructor, bypassing the SerializerBuildHelper/DeserializerBuildHelper path.
   */
  private class FhirAgnosticEncoder[T](
                                        override val dataType: DataType,
                                        override val clsTag: ClassTag[T]
                                      ) extends AgnosticEncoder[T] {
    override def isPrimitive: Boolean = false
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

    // Construct the ExpressionEncoder directly with our pre-built expression trees, rather than
    // going through the ExpressionEncoder.apply factory which would delegate back to
    // SerializerBuildHelper/DeserializerBuildHelper.
    val agnosticEncoder = new FhirAgnosticEncoder[Any](schema, ClassTag(fhirClass))
    new ExpressionEncoder(agnosticEncoder, serializerExpr, deserializerExpr)
  }
}
