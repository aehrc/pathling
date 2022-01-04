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

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import au.csiro.pathling.encoders2.SchemaTraversal.isSingular
import ca.uhn.fhir.context._

import scala.collection.convert.ImplicitConversions._

/**
 * A strategy to traverse a Fhir schema with [[SchemaTraversal]].
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
trait SchemaProcessor[DT, SF] {

  /**
   * Builds a representation for an child element with resolved name.
   *
   * @param childDefinition   the HAPI child definition.
   * @param elementDefinition the HAPI element definition.
   * @param elementName       the element name.
   * @param compositeBuilder  the callback to build the representation of a composite.
   * @return the representation of of the named child element.
   */
  def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                 compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): Seq[SF]

  /**
   * Determines if the representation of a child should be included in the representation of its composite.
   *
   * @param definition      the HAPI definition of a composite.
   * @param childDefinition the HAPI definition of the composite child.
   * @return true if the child representation should be included.
   */
  def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean

  /**
   * Combines the representations of the choice options to the representation of the choice.
   *
   * @param choiceDefinition the HAPI choice child definition.
   * @param optionValues     the list of representations of choice options.
   * @return the representation of the choice element.
   */
  def combineChoiceOptions(choiceDefinition: RuntimeChildChoiceDefinition, optionValues: Seq[Seq[SF]]): Seq[SF] = optionValues.flatten

  /**
   * Builds the representation of the composite from the representations of its elements.
   *
   * @param definition the HAPI definition of a composite
   * @param fields     the list of representations of the composite elements.
   * @return the representation of the composite.
   */
  def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_], fields: Seq[SF]): DT


  // explicit control of context switching

  /**
   * Called before the traversal enters a child. Allows to substitute the processor used to travers the child.
   *
   * @param childDefinition the HAPI child definition.
   * @return the processor to be used to traverse the child.
   */
  def beforeEnterChild(childDefinition: BaseRuntimeChildDefinition): SchemaProcessor[DT, SF] = this

  /**
   * Called before the traversal enters a choice option. Allows to substitute the processor used to travers the option.
   *
   * @param choiceDefinition the HAPI choice child definition.
   * @param optionName       the full name of the option element.
   * @return the processor to be used to traverse the option element.
   */
  def beforeEnterChoiceOption(choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): SchemaProcessor[DT, SF] = this
}

/**
 * Encapsulates the traversal a Fhir schema defined in HAPI.
 *
 * @param maxNestingLevel the maximum nesting levels to traverse.
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
sealed class SchemaTraversal[DT, SF](maxNestingLevel: Int) {

  /**
   * Creates the representation of given resource.
   *
   * @param proc               the processor to use to create the  representation.
   * @param resourceDefinition the HAPI definition of the resource
   * @return the representation of the resource.
   */
  def processResource(proc: SchemaProcessor[DT, SF], resourceDefinition: RuntimeResourceDefinition): DT = {
    EncodingContext.runWithContext {
      processComposite(proc, resourceDefinition)
    }
  }

  /**
   * Creates the representation of given composite.
   *
   * @param proc       the processor to use to create the the processor to use to create the resource representation. representation.
   * @param definition the HAPI definition of the composite.
   * @return the representation of the composite.
   */
  def processComposite(proc: SchemaProcessor[DT, SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    EncodingContext.withDefinition(definition) {
      val fields: Seq[SF] = definition
        .getChildren
        .filter(proc.shouldExpandChild(definition, _))
        .flatMap(enterChild(proc, _))
      proc.buildComposite(definition, fields)
    }
  }


  private def enterChild(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Nil
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        enterChoiceChild(proc.beforeEnterChild(choiceDefinition), choiceDefinition)
      case _ =>
        enterElementChild(proc.beforeEnterChild(childDefinition), childDefinition)
    }
  }

  private def enterChoiceChild(proc: SchemaProcessor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition): Seq[SF] = {
    assert(isSingular(choiceDefinition), "Collections of choice elements are not supported")
    val childValues = getOrderedListOfChoiceTypes(choiceDefinition)
      .map(choiceDefinition.getChildNameByDatatype)
      .map(childName => enterChoiceChildOption(proc.beforeEnterChoiceOption(choiceDefinition, childName), choiceDefinition, childName))
    proc.combineChoiceOptions(choiceDefinition, childValues)
  }

  private def enterChoiceChildOption(proc: SchemaProcessor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): Seq[SF] = {
    val definition = choiceDefinition.getChildByName(optionName)
    enterElement(proc, choiceDefinition, definition, optionName)
  }

  private def enterElementChild(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    val elementName = childDefinition.getElementName
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    enterElement(proc, childDefinition, definition, elementName)
  }

  private def enterElement(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition,
                           elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[SF] = {
    if (shouldExpand(elementDefinition)) {
      proc.buildValue(childDefinition, elementDefinition, elementName, processComposite)
    } else {
      Nil
    }
  }

  private def shouldExpand(definition: BaseRuntimeElementDefinition[_]): Boolean = {
    EncodingContext.currentNestingLevel(definition) <= maxNestingLevel
  }
}

/**
 * Companion object for [[SchemaTraversal]]
 */
object SchemaTraversal {
  /**
   * Checks if the child definition represents a collection
   *
   * @param childDefinition the HAPI child definition.
   * @return true is the child is a collection.
   */
  def isCollection(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax != 1
  }

  /**
   * Checks if the child definition represents a single element (not a collection)
   *
   * @param childDefinition the HAPI child definition.
   * @return true is the child is NOT a collection.
   */
  def isSingular(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax == 1
  }
}