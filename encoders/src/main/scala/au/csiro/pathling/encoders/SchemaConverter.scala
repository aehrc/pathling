package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.datatypes.SchemaConfig
import ca.uhn.fhir.context.{RuntimeChildChoiceDefinition, RuntimeResourceDefinition}
import org.apache.spark.sql.types.StructType
import org.hl7.fhir.instance.model.api.{IBase, IBaseResource}

import scala.collection.convert.ImplicitConversions._

trait SchemaConverter extends SchemaConfig {
  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {
    resourceSchema(fhirContext.getResourceDefinition(resourceClass))
  }

  def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType
}


object SchemaConverter {
  /**
   * Returns a deterministically ordered list of child types of choice.
   *
   * @param choice the choice child definition.
   * @return ordered list of child types of choice.
   */
  def getOrderedListOfChoiceTypes(choice: RuntimeChildChoiceDefinition): Seq[Class[_ <: IBase]] = {
    choice.getValidChildTypes.toList.sortBy(_.getTypeName())
  }
}
