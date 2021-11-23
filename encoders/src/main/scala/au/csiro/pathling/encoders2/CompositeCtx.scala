package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.EncodingContext
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceTypes
import ca.uhn.fhir.context._

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`


trait SchemaVisitor[DT, SF] {
  def visitComposite(ctx: CompositeCtx[DT, SF]): DT = {
    ctx.proceed(this)
  }

  def visitChild(ctx: ChildCtx[DT, SF]): Seq[SF] = {
    ctx.proceed(this)
  }

  def buildComposite(fields: Seq[SF], value: CompositeCtx[DT, SF]): DT

  def visitChoiceChild(ctx: ChoiceChildCtx[DT, SF]): Seq[SF]

  def visitElementChild(ctx: ElementChildCtx[DT, SF]): Seq[SF]

  def visitChoiceChildOption(value: ChoiceChildOptionCtx[DT, SF]): Seq[SF]
  def aggregateChoice(value: ChoiceChildCtx[DT, SF], childValues: Seq[Seq[SF]]): Seq[SF] = ???
}


case class CompositeCtx[DT, SF](compositeDef: BaseRuntimeElementCompositeDefinition[_]) {
  def accept(visitor: SchemaVisitor[DT, SF]): DT = {
    visitor.visitComposite(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): DT = {
    EncodingContext.withDefinition(compositeDef) {
      val fields: Seq[SF] = compositeDef
        .getChildren
        //.filter(shouldExpandChild(definition, _))
        .flatMap(childDef => ChildCtx(childDef, compositeDef).accept(visitor))
      visitor.buildComposite(fields, this)
    }
  }
}

case class ChildCtx[DT, SF](childDefinition: BaseRuntimeChildDefinition,
                            compositeDef: BaseRuntimeElementCompositeDefinition[_]) {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChild(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    childDefinition match {
      case _: RuntimeChildContainedResources | _: RuntimeChildExtension => Nil
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        ChoiceChildCtx[DT, SF](choiceDefinition, compositeDef).accept(visitor)
      case _ =>
        ElementChildCtx[DT, SF](childDefinition, compositeDef).accept(visitor)
    }
  }
}

case class ChoiceChildCtx[DT, SF](choiceDefinition: RuntimeChildChoiceDefinition,
                                  compositeDef: BaseRuntimeElementCompositeDefinition[_]) {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChoiceChild(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    assert(choiceDefinition.getMax == 1, "Collections of choice elements are not supported")
    val childValues = getOrderedListOfChoiceTypes(choiceDefinition)
      .map(choiceDefinition.getChildNameByDatatype)
      .map(childName => ChoiceChildOptionCtx(childName, choiceDefinition,compositeDef).accept(visitor))
    visitor.aggregateChoice(this, childValues)
  }
}

case class ChoiceChildOptionCtx[DT, SF](childName: String,
                                        choiceDefinition: RuntimeChildChoiceDefinition,
                                        compositeDef: BaseRuntimeElementCompositeDefinition[_]) {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChoiceChildOption(this)
  }
}

case class ElementChildCtx[DT, SF](childDefinition: BaseRuntimeChildDefinition,
                                   compositeDef: BaseRuntimeElementCompositeDefinition[_]) {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitElementChild(this)
  }
}






