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
import au.csiro.pathling.encoders.SchemaConverter.getOrderedListOfChoiceChildNames
import au.csiro.pathling.encoders2.SchemaVisitor.isSingular
import ca.uhn.fhir.context._
import org.hl7.fhir.r4.model.Patient

import scala.collection.convert.ImplicitConversions._


/**
 * A visitor for HAPPY Fhir schema traversal
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
trait SchemaVisitor[DT, SF] {

  def visitElement(value: ElementNode[DT, SF]): Seq[SF]

  def aggregateComposite(ctx: CompositeNode[DT, SF], sfs: Seq[SF]): DT

  def combineChoiceElements(ctx: ChoiceChildNode[DT, SF], seq: Seq[Seq[SF]]): Seq[SF]

  def visitChoiceChild(value: ChoiceChildNode[DT, SF]): Seq[SF] = {
    combineChoiceElements(value, value.visitChildren(this))
  }

  def visitElementChild(value: ElementChildNode[DT, SF]): Seq[SF] = {
    value.visitChildren(this)
  }

  def visitChild(value: ChildNode[DT, SF]): Seq[SF] = {
    value.proceed(this)
  }

  def visitComposite(value: CompositeNode[DT, SF]): DT = {
    aggregateComposite(value, value.visitChildren(this))
  }

  def visitResource(resourceNode: ResourceNode[DT, SF]): DT = {
    resourceNode.proceed(this)
  }
}

object SchemaVisitor {
  def traverseResource[DT, SF](resourceDefinition: RuntimeResourceDefinition, visitor: SchemaVisitor[DT, SF]): DT = {
    // TODO: NOT SURE where is the best place to put it
    // Not if it will be needed anymore if I can do recursive contexts
    EncodingContext.runWithContext {
      ResourceNode(resourceDefinition).accept(visitor)
    }
  }

  def traverseComposite[DT, SF](compositeDefinition: BaseRuntimeElementCompositeDefinition[_], visitor: SchemaVisitor[DT, SF]): DT = {
    CompositeNode(compositeDefinition).accept(visitor)
  }

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


trait FhirNode[DT, SF] {
}

trait TypeNode[DT, SF] extends FhirNode[DT, SF] {
  def accept(visitor: SchemaVisitor[DT, SF]): DT
}

trait FieldNode[DT, SF] extends FhirNode[DT, SF] {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF]
}

/**
 *
 * @param resourceDefinition
 * @tparam DT
 * @tparam SF
 */
case class ResourceNode[DT, SF](resourceDefinition: RuntimeResourceDefinition) extends TypeNode[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): DT = {
    visitor.visitResource(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): DT = {
    toComposite.accept(visitor)
  }

  def toComposite: CompositeNode[DT, SF] = {
    CompositeNode[DT, SF](resourceDefinition)
  }
}

case class CompositeNode[DT, SF](definition: BaseRuntimeElementCompositeDefinition[_]) extends TypeNode[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): DT = {
    visitor.visitComposite(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    definition
      .getChildren
      .flatMap(toChild(_).accept(visitor))
  }

  def toChild(childDefinition: BaseRuntimeChildDefinition): ChildNode[DT, SF] = {
    ChildNode[DT, SF](childDefinition, definition)
  }
}

case class ChildNode[DT, SF](childDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldNode[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChild(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    childDefinition match {
      case _: RuntimeChildContainedResources => Nil
      // we need to handle extension before choice as it is of this type but requires
      // the same handling as element child
      case _: RuntimeChildExtension =>
        ElementChildNode(childDefinition, compositeDefinition).accept(visitor)
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        ChoiceChildNode(choiceDefinition, compositeDefinition).accept(visitor)
      case _ =>
        ElementChildNode(childDefinition, compositeDefinition).accept(visitor)
    }
  }
}

case class ElementChildNode[DT, SF](childDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldNode[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitElementChild(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    val elementName = childDefinition.getElementName
    val definition = childDefinition.getChildByName(childDefinition.getElementName)
    ElementNode(elementName, childDefinition, compositeDefinition).accept(visitor)
  }
}


case class ChoiceChildNode[DT, SF](choiceDefinition: RuntimeChildChoiceDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldNode[DT, SF] {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChoiceChild(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[Seq[SF]] = {

    assert(isSingular(choiceDefinition), "Collections of choice elements are not supported")
    getOrderedListOfChoiceChildNames(choiceDefinition)
      .map(childName => ElementNode(childName, choiceDefinition, compositeDefinition).accept(visitor))
  }
}

case class ElementNode[DT, SF](elementName: String, childDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldNode[DT, SF] {

  lazy val elementDefinition: BaseRuntimeElementDefinition[_] = childDefinition.getChildByName(elementName)

  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitElement(this)
  }
}
