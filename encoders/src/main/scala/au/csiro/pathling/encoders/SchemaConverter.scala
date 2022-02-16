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

import ca.uhn.fhir.context.{FhirContext, RuntimeChildChoiceDefinition, RuntimeResourceDefinition}
import org.apache.spark.sql.types.StructType
import org.hl7.fhir.instance.model.api.{IBase, IBaseResource}

import scala.collection.convert.ImplicitConversions._

/**
 * The converter from FHIR schemas to (spark) SQL schemas.
 */
trait SchemaConverter {

  def fhirContext: FhirContext

  /**
   * Returns the spark (SQL) schema that represents the given FHIR resource class.
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {
    resourceSchema(fhirContext.getResourceDefinition(resourceClass))
  }

  /**
   * Returns the (spark) SQL schema that represents the given FHIR resource definition.
   *
   * @param resourceDefinition the FHIR resource definition.
   * @return the schema as a Spark StructType
   */
  def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType
}


/**
 * Companion object for [[SchemaConverter]]
 */
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
