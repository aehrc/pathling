package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.SchemaConverter
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
private[encoders2] object EncoderBuilder2 {

  /**
   * Returns an encoder for the FHIR resource implemented by the given class
   *
   * @param definition The FHIR resource definition
   * @param contained  The FHIR resources to be contained to the given definition
   * @return An ExpressionEncoder for the resource
   */

  def of(definition: BaseRuntimeElementCompositeDefinition[_],
         context: FhirContext,
         mappings: DataTypeMappings,
         converter: SchemaConverter,
         contained: mutable.Buffer[BaseRuntimeElementCompositeDefinition[_]] = mutable.Buffer.empty): ExpressionEncoder[_] = {

    val fhirClass = definition.getImplementingClass
    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)

    //    val encoderBuilder = new EncoderBuilder(context,
    //      mappings,
    //      converter)

    // TODO: adjust nesting level here
    val serializerBuilder = new SerializerBuilder2(mappings, context, 0)
    val serializers = serializerBuilder.enterComposite(inputObject, definition)
    val deserializer = Literal(0)
    //      EncodingContext.runWithContext {
    //      encoderBuilder.compositeToDeserializer(definition, None, contained)
    //    }

    new ExpressionEncoder(
      serializers,
      deserializer,
      ClassTag(fhirClass))
  }
}