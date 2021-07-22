/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
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
   * Returns the fields used to represent the given element definition.
   *
   * @param elementDefinition the definition of the element
   * @param elementName       the name of the element
   * @param isCollection      is the element is part of a collectio (max allowed occurence > 1)
   * @return the list of fields for the element representation.
   */
  private def elementToFields(elementDefinition: BaseRuntimeElementDefinition[_], elementName: String, isCollection: Boolean): Seq[StructField] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    assert(customEncoder.isEmpty || !isCollection, "Collections are not supported for custom encoders")
    customEncoder.map(_.schema).getOrElse {
      val childType = elementDefinition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToStructType(composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive)
        case _: RuntimePrimitiveDatatypeNarrativeDefinition => DataTypes.StringType
        case _: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => DataTypes.StringType
      }
      if (isCollection) {
        Seq(StructField(elementName, ArrayType(childType)))
      } else {
        Seq(StructField(elementName, childType))
      }
    }
  }

  /**
   * Returns the fields used to represent the given child definition. In most cases this
   * will contain a single element, but in special cases like Choice elements, it will
   * contain a field for each possible FHIR choice.
   */
  private[encoders] def childToFields(childDefinition: BaseRuntimeChildDefinition): Seq[StructField] = {
    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Empty
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        assert(childDefinition.getMax == 1, "Collections of choice elements are not supported")
        getOrderedListOfChoiceTypes(choiceDefinition)
          .map(choiceDefinition.getChildNameByDatatype)
          .flatMap(childName => elementToFields(choiceDefinition.getChildByName(childName), childName, isCollection = false))
      case _ =>
        val definition = childDefinition.getChildByName(childDefinition.getElementName)
        elementToFields(definition, childDefinition.getElementName, isCollection = childDefinition.getMax != 1)
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
