package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.SchemaTraversal.{isCollection, isSingular}
import ca.uhn.fhir.context.{BaseRuntimeElementDefinition, _}

import scala.collection.convert.ImplicitConversions._


trait SchemaVisitor[DT, SF] {

  def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                 compositeBuilder: (SchemaVisitor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): Seq[SF]

  def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean

  def aggregateChoice(choiceDefinition: RuntimeChildChoiceDefinition, optionValues: Seq[Seq[SF]]): Seq[SF] = optionValues.flatten

  def buildComposite(fields: Seq[SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT


  // explicit control of context switching
  def enterChild(childDefinition: BaseRuntimeChildDefinition): SchemaVisitor[DT, SF] = this

  def enterChoiceOption(choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): SchemaVisitor[DT, SF] = this
}


trait SchemaVisitorWithTypeMappings[DT, SF] extends SchemaVisitor[DT, SF] {
  def dataTypeMappings: DataTypeMappings

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
    !dataTypeMappings.skipField(definition, childDefinition)
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaVisitor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): Seq[SF] = {
    val value = if (isCollection(childDefinition)) {
      buildArrayValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    } else {
      buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    }
    Seq(buildElement(elementName, value, elementDefinition))
  }


  def buildSimpleValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                       compositeBuilder: (SchemaVisitor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT = {
    if (childDefinition.isInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition]) {
      buildEnumDatatype(elementDefinition.asInstanceOf[RuntimePrimitiveDatatypeDefinition],
        childDefinition.asInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition])
    } else {
      elementDefinition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeBuilder(this, composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(primitive)
        case xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org)
        case _: RuntimePrimitiveDatatypeNarrativeDefinition => buildPrimitiveDatatypeNarrative
      }
    }
  }

  def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                      compositeBuilder: (SchemaVisitor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT

  def buildElement(elementName: String, elementType: DT, definition: BaseRuntimeElementDefinition[_]): SF

  def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DT

  def buildPrimitiveDatatypeNarrative: DT

  def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DT

  def buildEnumDatatype(enumDefinition: RuntimePrimitiveDatatypeDefinition,
                        enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): DT = {
    buildPrimitiveDatatype(enumDefinition)
  }
}

class SchemaTraversal[DT, SF](maxNestingLevel: Int) {

  /**
   * Returns the Spark schema that represents the given FHIR resource
   *
   * @param resourceDefinition The definition of the FHIR resource
   * @return The schema as a Spark StructType
   */
  def enterResource(ctx: SchemaVisitor[DT, SF], resourceDefinition: RuntimeResourceDefinition): DT = {
    EncodingContext.runWithContext {
      enterComposite(ctx, resourceDefinition)
    }
  }

  def enterComposite(ctx: SchemaVisitor[DT, SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    visitComposite(ctx, definition)
  }

  def visitComposite(ctx: SchemaVisitor[DT, SF], definition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    //println(s"visitComposite: ${definition}")
    EncodingContext.withDefinition(definition) {
      val fields: Seq[SF] = definition
        .getChildren
        .filter(ctx.shouldExpandChild(definition, _))
        .flatMap(visitChild(ctx, _))
      ctx.buildComposite(fields, definition)
    }
  }

  def visitChild(ctx: SchemaVisitor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    //println(s"visitChild: ${childDefinition}")

    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Nil
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        visitChoiceChild(ctx.enterChild(choiceDefinition), choiceDefinition)
      case _ =>
        visitElementChild(ctx.enterChild(childDefinition), childDefinition)
    }
  }

  def visitChoiceChild(ctx: SchemaVisitor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition): Seq[SF] = {
    assert(isSingular(choiceDefinition), "Collections of choice elements are not supported")
    val childValues = getOrderedListOfChoiceTypes(choiceDefinition)
      .map(choiceDefinition.getChildNameByDatatype)
      .map(childName => visitChoiceChildOption(ctx.enterChoiceOption(choiceDefinition, childName), choiceDefinition, childName))
    ctx.aggregateChoice(choiceDefinition, childValues)
  }

  def visitChoiceChildOption(ctx: SchemaVisitor[DT, SF], choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): Seq[SF] = {
    val definition = choiceDefinition.getChildByName(optionName)
    visitElement(ctx, choiceDefinition, definition, optionName)
  }

  def visitElementChild(ctx: SchemaVisitor[DT, SF], childDefinition: BaseRuntimeChildDefinition): Seq[SF] = {
    val elementName = childDefinition.getElementName
    //println(s"visitNamedChild: ${childDefinition}.${elementName}")
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    visitElement(ctx, childDefinition, definition, elementName)
  }

  def visitElement(ctx: SchemaVisitor[DT, SF], childDefinition: BaseRuntimeChildDefinition,
                   elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[SF] = {
    //println(s"visitElement: ${elementDefinition}.${elementName}[${valueBuilder}]")
    if (shouldExpand(elementDefinition)) {
      // here we need to plug custom encoders in one way or another
      // so this here should essentialy be delegated to the visitor
      // it should be reponsible for doing the possile custom encoding
      ctx.buildValue(childDefinition, elementDefinition, elementName, visitComposite)
    } else {
      Nil
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


object SchemaTraversal {
  def isCollection(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax != 1
  }

  def isSingular(childDefinition: BaseRuntimeChildDefinition): Boolean = {
    childDefinition.getMax == 1
  }
}