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

package au.csiro.pathling.encoders1

import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders.{EncodingContext, SchemaConverter}
import ca.uhn.fhir.context.{RuntimeChildChoiceDefinition, _}
import org.apache.spark.sql.types.{BooleanType => _, DateType => _, StringType => _, _}

import scala.collection.convert.ImplicitConversions._
import scala.collection.immutable.Stream.Empty

/**
 * Extracts a Spark schema based on a FHIR data model.
 */
class SchemaConverter1(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings, val maxNestingLevel: Int) extends SchemaConverter {

  def this(fhirContext: FhirContext, dataTypeMappings: DataTypeMappings) {
    this(fhirContext, dataTypeMappings, 0)
  }

  override def supportsExtensions: Boolean = false

  override def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    EncodingContext.runWithContext {
      compositeToStructType(resourceDefinition)
    }
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @param definition The FHIR definition of a composite type.
   * @return The schema as a Spark StructType
   */
  private[encoders1] def compositeToStructType(definition: BaseRuntimeElementCompositeDefinition[_]): StructType = {
    EncodingContext.withDefinition(definition) {
      // Map to (name, value, name, value) expressions for child elements.
      val fields: Seq[StructField] = definition
        .getChildren
        .filter(child => !dataTypeMappings.skipField(definition, child))
        .flatMap(childToFields)
      StructType(fields)
    }
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @param definition The FHIR definition of a composite type.
   * @return The schema as a Spark StructType
   */
  private[encoders1] def shouldExpand(definition: BaseRuntimeElementDefinition[_]): Boolean = {
    EncodingContext.currentNestingLevel(definition) <= maxNestingLevel
  }

  /**
   * Returns the fields used to represent the given element definition.
   *
   * @param elementDefinition the definition of the element
   * @param elementName       the name of the element
   * @param isCollection      is the element is part of a collection (max allowed occurrence > 1)
   * @return the list of fields for the element representation.
   */
  private def elementToFields(elementDefinition: BaseRuntimeElementDefinition[_], elementName: String, isCollection: Boolean): Seq[StructField] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    assert(customEncoder.isEmpty || !isCollection,
      "Collections are not supported for custom encoders for: " + elementName + "-> " + elementDefinition)
    //noinspection ScalaDeprecation
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
   * Returns the fields used to given named child.
   *
   * @param childDefinition the of the child.
   * @param childName       the name of the child.
   * @return the list of fields for the named child representation.
   */
  private def namedChildToFields(childDefinition: BaseRuntimeChildDefinition, childName: String): Seq[StructField] = {

    val definition = childDefinition.getChildByName(childName)
    if (shouldExpand(definition)) {
      elementToFields(definition, childName, isCollection = childDefinition.getMax != 1)
    } else {
      Empty
    }
  }

  /**
   * Returns the fields used to represent the given child definition. In most cases this
   * will contain a single element, but in special cases like Choice elements, it will
   * contain a field for each possible FHIR choice.
   */
  private def childToFields(childDefinition: BaseRuntimeChildDefinition): Seq[StructField] = {
    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Empty
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        assert(childDefinition.getMax == 1, "Collections of choice elements are not supported")
        getOrderedListOfChoiceTypes(choiceDefinition)
          .map(choiceDefinition.getChildNameByDatatype)
          .flatMap(childName => namedChildToFields(choiceDefinition, childName))
      case _ =>
        namedChildToFields(childDefinition, childDefinition.getElementName)
    }
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
  //noinspection ScalaUnusedSymbol
  private def parentToStructType(definition: BaseRuntimeElementCompositeDefinition[_],
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
