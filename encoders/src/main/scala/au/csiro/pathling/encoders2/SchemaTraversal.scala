package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import ca.uhn.fhir.context._
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.collection.convert.ImplicitConversions._

abstract class SchemaTraversal[DT, SF, CTX](fhirContext: FhirContext, maxNestingLevel: Int) {

  def buildComposite(ctx: CTX, fields: Seq[SF]): DT

  def buildElement(elementName: String, elementType: DT, definition: BaseRuntimeElementDefinition[_]): SF

  def buildPrimitiveDatatype(ctx: CTX, primitive: RuntimePrimitiveDatatypeDefinition): DT

  def buildPrimitiveDatatypeNarrative: DT

  def buildPrimitiveDatatypeXhtmlHl7Org: DT


  def visitComposite(ctx: CTX, definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    println(s"visitComposite: ${definition}")
    EncodingContext.withDefinition(definition) {
      val fields: Seq[SF] = definition
        .getChildren
        .flatMap(visitChild(ctx, _))
      buildComposite(ctx, fields)
    }
  }

  def visitChild(ctx: CTX, childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    println(s"visitChild: ${childDefinition}")

    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Nil
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        visitChoiceChild(ctx, choiceDefinition)
      case _ =>
        visitElementChild(ctx, childDefinition)
    }
  }

  def visitChoiceChild(ctx: CTX, choiceDefinition: RuntimeChildChoiceDefinition): Seq[SF] = {
    assert(choiceDefinition.getMax == 1, "Collections of choice elements are not supported")
    getOrderedListOfChoiceTypes(choiceDefinition)
      .map(choiceDefinition.getChildNameByDatatype)
      .flatMap(childName => visitChoiceChildOption(ctx, choiceDefinition, childName))
  }

  def visitChoiceChildOption(ctx: CTX, choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): Seq[SF] = {
    val definition = choiceDefinition.getChildByName(optionName)
    visitElement(ctx, definition, optionName, isCollection = choiceDefinition.getMax != 1)
  }

  def visitElementChild(ctx: CTX, childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    val elementName = childDefinition.getElementName
    println(s"visitNamedChild: ${childDefinition}.${elementName}")
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    visitElement(ctx, definition, elementName, isCollection = childDefinition.getMax != 1)
  }

  def visitElement(ctx: CTX, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String, isCollection: Boolean): Seq[SF] = {
    println(s"visitElement: ${elementDefinition}.${elementName}[${isCollection}]")
    if (shouldExpand(elementDefinition)) {
      val childType: DT = elementDefinition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => visitComposite(ctx, composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(ctx, primitive)
        case _: RuntimePrimitiveDatatypeNarrativeDefinition => buildPrimitiveDatatypeNarrative
        case _: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org
      }
      Seq(buildElement(elementName, childType, elementDefinition))
    } else {
      Nil
    }
    // TODO: Need to add arrays somewhere here or above here
  }

  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def visitResource[T <: IBaseResource](ctx: CTX, resourceClass: Class[T]): DT = {
    EncodingContext.runWithContext {
      val definition = fhirContext.getResourceDefinition(resourceClass)
      visitComposite(ctx, definition)
    }
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @param definition The FHIR definition of a composite type.
   * @return The schema as a Spark StructType
   */
  def shouldExpand(definition: BaseRuntimeElementDefinition[_]): Boolean = {
    EncodingContext.currentNestingLevel(definition) <= maxNestingLevel
  }

}
