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

package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.UnsupportedResourceError
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
object EncoderBuilder2 {

  val UNSUPPORTED_RESOURCES: Set[String] = Set("Parameters",
    "Task", "StructureDefinition", "StructureMap", "Bundle")

  /**
   * Returns an encoder for the FHIR resource implemented by the given class
   *
   * @param resourceDefinition The FHIR resource definition
   * @param fhirContext        the FHIR context to use
   * @param mappings           the data type mappings to use
   * @param maxNestingLevel    the max nesting level to use to expand recursive data types.
   *                           Zero means that fields of type T are skipped in a composite od type T.
   * @param enableExtensions   true is support for extensions should be enabled.
   * @return An ExpressionEncoder for the resource
   */

  def of(resourceDefinition: RuntimeResourceDefinition,
         fhirContext: FhirContext,
         mappings: DataTypeMappings,
         maxNestingLevel: Int,
         enableExtensions: Boolean): ExpressionEncoder[_] = {

    if (UNSUPPORTED_RESOURCES.contains(resourceDefinition.getName)) {
      throw new UnsupportedResourceError(s"Encoding is not supported for resource: ${resourceDefinition.getName}")
    }

    val fhirClass = resourceDefinition
      .asInstanceOf[BaseRuntimeElementDefinition[_]].getImplementingClass
    val schemaConverter = new SchemaConverter2(fhirContext, mappings, maxNestingLevel, enableExtensions)
    val serializerBuilder = SerializerBuilder2(schemaConverter)
    val deserializerBuilder = DeserializerBuilder2(schemaConverter)
    new ExpressionEncoder(
      serializerBuilder.buildSerializer(resourceDefinition),
      deserializerBuilder.buildDeserializer(resourceDefinition),
      ClassTag(fhirClass))
  }
}