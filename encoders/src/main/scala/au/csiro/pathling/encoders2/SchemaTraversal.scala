package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import au.csiro.pathling.encoders2.SchemaTraversal.isSingular
import ca.uhn.fhir.context.{BaseRuntimeElementDefinition, _}

import scala.collection.convert.ImplicitConversions._


trait SchemaProcessor[DT, SF] {

  def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                 compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): Seq[SF]

  def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean

  def aggregateChoice(choiceDefinition: RuntimeChildChoiceDefinition, optionValues: Seq[Seq[SF]]): Seq[SF] = optionValues.flatten

  def buildComposite(fields: Seq[SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT


  // explicit control of context switching
  def beforeEnterChild(childDefinition: BaseRuntimeChildDefinition): SchemaProcessor[DT, SF] = this

  def beforeEnterChoiceOption(choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): SchemaProcessor[DT, SF] = this
}

class SchemaTraversal[DT, SF](maxNestingLevel: Int) {

  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceDefinition The definition of the FHIR resource
   * @return The schema as a Spark StructType
   */
  def enterResource(proc: SchemaProcessor[DT, SF], resourceDefinition: RuntimeResourceDefinition): DT = {
    EncodingContext.runWithContext {
      enterComposite(proc, resourceDefinition)
    }
  }

  def enterComposite(proc: SchemaProcessor[DT, SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    EncodingContext.withDefinition(definition) {
      val fields: Seq[SF] = definition
        .getChildren
        .filter(proc.shouldExpandChild(definition, _))
        .flatMap(enterChild(proc, _))
      proc.buildComposite(fields, definition)
    }
  }


  def enterChild(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Nil
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        enterChoiceChild(proc.beforeEnterChild(choiceDefinition), choiceDefinition)
      case _ =>
        enterElementChild(proc.beforeEnterChild(childDefinition), childDefinition)
    }
  }

  def enterChoiceChild(proc: SchemaProcessor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition): Seq[SF] = {
    assert(isSingular(choiceDefinition), "Collections of choice elements are not supported")
    val childValues = getOrderedListOfChoiceTypes(choiceDefinition)
      .map(choiceDefinition.getChildNameByDatatype)
      .map(childName => enterChoiceChildOption(proc.beforeEnterChoiceOption(choiceDefinition, childName), choiceDefinition, childName))
    proc.aggregateChoice(choiceDefinition, childValues)
  }

  def enterChoiceChildOption(proc: SchemaProcessor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): Seq[SF] = {
    val definition = choiceDefinition.getChildByName(optionName)
    enterElement(proc, choiceDefinition, definition, optionName)
  }

  def enterElementChild(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    val elementName = childDefinition.getElementName
    //println(s"visitNamedChild: ${childDefinition}.${elementName}")
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    enterElement(proc, childDefinition, definition, elementName)
  }

  def enterElement(proc: SchemaProcessor[DT, SF], childDefinition: BaseRuntimeChildDefinition,
                   elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[SF] = {
    if (shouldExpand(elementDefinition)) {
      proc.buildValue(childDefinition, elementDefinition, elementName, enterComposite)
    } else {
      Nil
    }
  }

  def shouldExpand(definition: BaseRuntimeElementDefinition[_]): Boolean = {
    EncodingContext.currentNestingLevel(definition) <= maxNestingLevel
  }
}

object SchemaTraversal {
  def isCollection(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax != 1
  }

  def isSingular(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax == 1
  }
}