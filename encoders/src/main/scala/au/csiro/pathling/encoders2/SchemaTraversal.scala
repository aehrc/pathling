package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import ca.uhn.fhir.context.{BaseRuntimeElementDefinition, _}
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.collection.convert.ImplicitConversions._

abstract class SchemaTraversal[DT, SF, CTX](fhirContext: FhirContext, maxNestingLevel: Int) {

  def buildComposite(ctx: CTX, fields: Seq[SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT

  def buildElement(elementName: String, elementType: DT, definition: BaseRuntimeElementDefinition[_]): SF

  def buildPrimitiveDatatype(ctx: CTX, primitive: RuntimePrimitiveDatatypeDefinition): DT

  def buildPrimitiveDatatypeNarrative(ctx: CTX): DT

  def buildPrimitiveDatatypeXhtmlHl7Org(ctx: CTX, xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DT

  def buildArrayTransformer(arrayDefinition: BaseRuntimeChildDefinition): (CTX, BaseRuntimeElementDefinition[_]) => DT

  def buildValue(ctx: CTX, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                 valueBuilder: (CTX, BaseRuntimeElementDefinition[_]) => DT): Seq[SF] = {
    Seq(buildElement(elementName,
      valueBuilder(ctx, elementDefinition),
      elementDefinition))
  }

  def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean

  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def enterResource[T <: IBaseResource](ctx: CTX, resourceClass: Class[T]): DT = {
    val definition = fhirContext.getResourceDefinition(resourceClass)
    enterComposite(ctx, definition)
  }

  def enterComposite(ctx: CTX, definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    EncodingContext.runWithContext {
      visitComposite(ctx, definition)
    }
  }

  def visitComposite(ctx: CTX, definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    //println(s"visitComposite: ${definition}")
    EncodingContext.withDefinition(definition) {
      val fields: Seq[SF] = definition
        .getChildren
        .filter(shouldExpandChild(definition, _))
        .flatMap(visitChild(ctx, _))
      buildComposite(ctx, fields, definition)
    }
  }

  def visitChild(ctx: CTX, childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    //println(s"visitChild: ${childDefinition}")

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
    visitElement(ctx, definition, optionName, visitElementValue)
  }

  def visitElementChild(ctx: CTX, childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    val elementName = childDefinition.getElementName
    //println(s"visitNamedChild: ${childDefinition}.${elementName}")
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    val valueBuilder: (CTX, BaseRuntimeElementDefinition[_]) => DT = (childDefinition.getMax != 1) match {
      case true => buildArrayTransformer(childDefinition)
      case false => visitElementValue
    }
    visitElement(ctx, definition, elementName, valueBuilder)
  }

  def visitElement(ctx: CTX, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                   valueBuilder: (CTX, BaseRuntimeElementDefinition[_]) => DT): Seq[SF] = {
    //println(s"visitElement: ${elementDefinition}.${elementName}[${valueBuilder}]")
    if (shouldExpand(elementDefinition)) {
      // here we need to plug custom encoders in one way or another
      // so this here should essentialy be delegated to the visitor
      // it should be reponsible for doing the possile custom encoding
      buildValue(ctx, elementDefinition, elementName, valueBuilder)
    } else {
      Nil
    }
  }

  def visitElementValue(ctx: CTX, elementDefinition: BaseRuntimeElementDefinition[_]): DT = {
    elementDefinition match {
      case composite: BaseRuntimeElementCompositeDefinition[_] => visitComposite(ctx, composite)
      case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(ctx, primitive)
      case _: RuntimePrimitiveDatatypeNarrativeDefinition => buildPrimitiveDatatypeNarrative(ctx)
      case xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org(ctx, xhtmlHl7Org)
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
