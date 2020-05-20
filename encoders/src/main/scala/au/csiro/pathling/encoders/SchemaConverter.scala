/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context.{RuntimeChildChoiceDefinition, _}
import org.apache.spark.sql.types.{BooleanType => _, DateType => _, StringType => _, _}
import org.hl7.fhir.instance.model.api.{IBase, IBaseResource}

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream.Empty

/**
 * Extracts a Spark schema based on a FHIR data model.
 */
class SchemaConverter(fhirContext: FhirContext, dataTypeMappings: DataTypeMappings) {

  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {

    val definition = fhirContext.getResourceDefinition(resourceClass)

    compositeToStructType(definition)
  }

  /**
   * Returns the fields used to represent the given child definition. In most cases this
   * will contain a single element, but in special cases like Choice elements, it will
   * contain a field for each possible FHIR choice.
   */
  private def childToFields(childDefinition: BaseRuntimeChildDefinition): Seq[StructField] = {


    val customCoder = dataTypeMappings.customEncoder(childDefinition)
    if (customCoder.nonEmpty) {

      customCoder.get.schema

    } else if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      // Contained resources and extensions not yet supported.
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Empty

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val choice = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      // Iterate by types and then lookup the field names so we get the preferred
      // field name for the given type.
      for (fhirChildType <- getOrderedListOfChoiceTypes(choice)) yield {

        val childName = choice.getChildNameByDatatype(fhirChildType)

        val childType = choice.getChildByName(childName) match {

          // case reference if (reference.getImplementingClass == classOf[Reference]) => SchemaConverter.referenceSchema
          case composite: RuntimeCompositeDatatypeDefinition => compositeToStructType(composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive);
        }

        StructField(childName, childType)
      }

    } else {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)

      val childType = definition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToStructType(composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive)
        case narrative: RuntimePrimitiveDatatypeNarrativeDefinition => DataTypes.StringType
        case hl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => DataTypes.StringType
      }

      if (childDefinition.getMax != 1) {
        List(StructField(childDefinition.getElementName, ArrayType(childType)))
      } else {
        List(StructField(childDefinition.getElementName, childType))
      }
    }
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @param definition The FHIR definition of a composite type.
   * @return The schema as a Spark StructType
   */
  private[encoders] def compositeToStructType(definition: BaseRuntimeElementCompositeDefinition[_]): StructType = {

    // Map to (name, value, name, value) expressions for child elements.
    val fields: Seq[StructField] = definition
      .getChildren
      .filter(child => !dataTypeMappings.skipField(definition, child))
      .flatMap(childToFields)

    StructType(fields)
  }

  /**
   * Returns the Spark struct type used to encode the given parent FHIR composite and any optional
   * contained FHIR resources.
   *
   * @param definition The FHIR definition of the parent having a composite type.
   * @param contained  The FHIR definitions of resources contained to the parent having composite
   *                   types.
   * @return The schema of the parent as a Spark StructType
   */
  private[encoders] def parentToStructType(definition: BaseRuntimeElementCompositeDefinition[_],
                                           contained: Seq[BaseRuntimeElementCompositeDefinition[_]]): StructType = {

    val parent = compositeToStructType(definition)

    if (contained.nonEmpty) {
      val containedFields = contained.map(containedElement =>
        StructField(containedElement.getName,
          compositeToStructType(containedElement)))

      val containedStruct = StructType(containedFields)

      parent.add(StructField("contained", containedStruct))

    } else {

      parent
    }
  }
}

object SchemaConverter {
  /**
   * Returns a deterministically ordered list of child types of choice.
   *
   * @param choice
   * @return
   */
  def getOrderedListOfChoiceTypes(choice: RuntimeChildChoiceDefinition): Seq[Class[_ <: IBase]] = {
    choice.getValidChildTypes.toList.sortBy(_.getTypeName())
  }
}
