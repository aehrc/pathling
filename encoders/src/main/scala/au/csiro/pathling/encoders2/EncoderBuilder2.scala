package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
object EncoderBuilder2 {

  /**
   * Returns an encoder for the FHIR resource implemented by the given class
   *
   * @param resourceDefinition The FHIR resource definition
   * @param contained          The FHIR resources to be contained to the given definition
   * @return An ExpressionEncoder for the resource
   */

  def of(resourceDefinition: RuntimeResourceDefinition,
         fhirContext: FhirContext,
         mappings: DataTypeMappings,
         maxNestingLevel: Int,
         contained: mutable.Buffer[BaseRuntimeElementCompositeDefinition[_]] = mutable.Buffer.empty): ExpressionEncoder[_] = {

    val fhirClass = resourceDefinition
      .asInstanceOf[BaseRuntimeElementDefinition[_]].getImplementingClass
    // TODO: Add contained resources
    val serializerBuilder = new SerializerBuilder2(mappings, fhirContext, maxNestingLevel)
    // TODO: Move schema converter to deserializer or make it master of all configs
    val schemaConverter = new SchemaConverter2(fhirContext, mappings, maxNestingLevel)
    val deserializerBuilder = new DeserializerBuilder2(fhirContext, mappings, maxNestingLevel, schemaConverter)
    new ExpressionEncoder(
      serializerBuilder.buildSerializer(resourceDefinition),
      deserializerBuilder.buildDeserializer(resourceDefinition),
      ClassTag(fhirClass))
  }
}