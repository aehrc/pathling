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
import au.csiro.pathling.encoders2.SchemaVisitor.{getOrderedListOfChoiceChildNames, isSingular}
import ca.uhn.fhir.context._
import org.hl7.fhir.instance.model.api.IBase
import org.hl7.fhir.r4.model.PrimitiveType

import scala.collection.convert.ImplicitConversions._


/**
 * A visitor for HAPPY Fhir schema traversal
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
trait SchemaVisitor[DT, SF] {


  /**
   * Transforms the SF representations of the composite elements to the DT representation of the composite.
   *
   * @param compositeCtx the composite context.
   * @param sfs          the list of the SF representations of the composite elements.
   * @return the DT representation of the composite.
   */
  def aggregateComposite(compositeCtx: CompositeCtx[DT, SF], sfs: Seq[SF]): DT

  def combineChoiceElements(ctx: ChoiceChildCtx[DT, SF], seq: Seq[Seq[SF]]): Seq[SF]

  /**
   * Visitor method for a HAPPY  Element definition
   *
   * @param elementCtx the element context.
   * @return the list of the SF representations of the element.
   */
  def visitElement(elementCtx: ElementCtx[DT, SF]): Seq[SF]


  /**
   * Visitor method for HAPPY RuntimeChild definition of a choice.
   *
   * @param choiceChildCtx the choice child context.
   * @return the list of the SF representations of the elements of the child definition.
   */
  def visitChoiceChild(choiceChildCtx: ChoiceChildCtx[DT, SF]): Seq[SF] = {
    combineChoiceElements(choiceChildCtx, choiceChildCtx.visitChildren(this))
  }

  /**
   * Visitor method for HAPPY RuntimeChild definition with a single element.
   *
   * @param elementChildCtx child context.
   * @return the list of the SF representations of the elements of the child definition.
   */
  def visitElementChild(elementChildCtx: ElementChildCtx[DT, SF]): Seq[SF] = {
    elementChildCtx.visitChildren(this)
  }

  /**
   * Visitor method for HAPPY RuntimeChild definition.
   *
   * @param childCtx the child context.
   * @return the list of the SF representations of the elements of the child definition.
   */
  def visitChild(childCtx: ChildCtx[DT, SF]): Seq[SF] = {
    childCtx.proceed(this)
  }


  /**
   * Visitor method for HAPPY ElementComposite definition.
   *
   * @param compositeCtx the composite element context.
   * @return DT representation of the composite element.
   */
  def visitComposite(compositeCtx: CompositeCtx[DT, SF]): DT = {
    aggregateComposite(compositeCtx, compositeCtx.visitChildren(this))
  }

  /**
   * Visitor method for HAPPY Resource definitions.
   *
   * @param resourceCtx the resource context.
   * @return DT representation of the resource.
   */
  def visitResource(resourceCtx: ResourceCtx[DT, SF]): DT = {
    resourceCtx.proceed(this)
  }
}

object SchemaVisitor {
  def traverseResource[DT, SF](resourceDefinition: RuntimeResourceDefinition, visitor: SchemaVisitor[DT, SF]): DT = {
    // TODO: NOT SURE where is the best place to put it
    // Not if it will be needed anymore if I can do recursive contexts
    EncodingContext.runWithContext {
      ResourceCtx(resourceDefinition).accept(visitor)
    }
  }

  def traverseComposite[DT, SF](compositeDefinition: BaseRuntimeElementCompositeDefinition[_], visitor: SchemaVisitor[DT, SF]): DT = {
    CompositeCtx(compositeDefinition).accept(visitor)
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

  /**
   * Non primitive datatypes that are allowed in open choices like value[*].
   * As defined in:https://www.hl7.org/fhir/datatypes.html#open
   */
  val allowedOpenTypes: Set[Class[_]] = Set(
    // DataTypes
    classOf[org.hl7.fhir.r4.model.Address],
    classOf[org.hl7.fhir.r4.model.Age],
    classOf[org.hl7.fhir.r4.model.Annotation],
    classOf[org.hl7.fhir.r4.model.Attachment],
    classOf[org.hl7.fhir.r4.model.CodeableConcept],
    classOf[org.hl7.fhir.r4.model.Coding],
    classOf[org.hl7.fhir.r4.model.ContactPoint],
    classOf[org.hl7.fhir.r4.model.Count],
    classOf[org.hl7.fhir.r4.model.Distance],
    classOf[org.hl7.fhir.r4.model.Duration],
    classOf[org.hl7.fhir.r4.model.HumanName],
    classOf[org.hl7.fhir.r4.model.Identifier],
    classOf[org.hl7.fhir.r4.model.Money],
    classOf[org.hl7.fhir.r4.model.Period],
    classOf[org.hl7.fhir.r4.model.Quantity],
    classOf[org.hl7.fhir.r4.model.Range],
    classOf[org.hl7.fhir.r4.model.Ratio],
    classOf[org.hl7.fhir.r4.model.Reference],
    classOf[org.hl7.fhir.r4.model.SampledData],
    classOf[org.hl7.fhir.r4.model.Signature],
    classOf[org.hl7.fhir.r4.model.Timing],
    // MetaDataTypes
    classOf[org.hl7.fhir.r4.model.ContactDetail],
    classOf[org.hl7.fhir.r4.model.Contributor],
    classOf[org.hl7.fhir.r4.model.DataRequirement],
    classOf[org.hl7.fhir.r4.model.Expression],
    classOf[org.hl7.fhir.r4.model.ParameterDefinition],
    classOf[org.hl7.fhir.r4.model.RelatedArtifact],
    classOf[org.hl7.fhir.r4.model.TriggerDefinition],
    classOf[org.hl7.fhir.r4.model.UsageContext],
    // Special Types
    classOf[org.hl7.fhir.r4.model.Dosage],
    classOf[org.hl7.fhir.r4.model.Meta]
  )


  /**
   * Checks if the given class is a valid type for open elements types as defined in: [[https://build.fhir.org/datatypes.html#open]].
   * Note: this is needed because the HAPI implementation of open type element [[ca.uhn.fhir.context.RuntimeChildAny#getValidChildTypes]] returns
   * types not included in the specification such as [[org.hl7.fhir.r4.model.ElementDefinition]].
   *
   * @param cls the class of the type to checks.
   * @return true is given type is a valid open element type.
   */
  def isValidOpenElementType(cls: Class[_ <: IBase]): Boolean = {
    classOf[PrimitiveType[_]].isAssignableFrom(cls) || allowedOpenTypes.contains(cls)
  }

  /**
   * Returns a deterministically ordered list of child names of a choice.
   *
   * @param choice the choice child definition.
   * @return ordered list of child names of the choice.
   */
  def getOrderedListOfChoiceChildNames(choice: RuntimeChildChoiceDefinition): Seq[String] = {
    choice
      .getValidChildTypes
      .filter(cls => !choice.isInstanceOf[RuntimeChildAny] || isValidOpenElementType(cls))
      .toList
      .sortBy(_.getTypeName())
      .map(choice.getChildNameByDatatype)
      .distinct

    // we need to use `distinct` as the list of allowed types may contain profiles of a type
    // that resolve to same childName. e.g. [[org.hl7.fhir.r4.model.MoneyQuantity]] and
    // [[org.hl7.fhir.r4.model.SimpleQuantity]] are both profiles of [[org.hl7.fhir.r4.model.Quantity]]
    // and resolve to the child name of `valueQuantity`.
    // They both and the [[org.hl7.fhir.r4.model.Quantity]] appear in the list of allowed types
    // for RuntimeChildAny. This case should be actually addressed by filtering with [[#isValidOpenElementType]]
    // but there might be other types of choices that have similar issue.
  }
}


/**
 * Base trait for all visitor contexts.
 *
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
trait VisitorCtx[DT, SF]


/**
 * Base trait for visitor context that produce DT representations.
 *
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
trait TypeVisitorCtx[DT, SF] extends VisitorCtx[DT, SF] {
  /**
   * Accept the visitor and dispatch the call to the appropriate 'visit' method.
   *
   * @param visitor the visitor to dispatch the call to.
   * @return the DT representation of this context.
   */
  def accept(visitor: SchemaVisitor[DT, SF]): DT
}

/**
 * Base trait for visitor context that produce ST representations.
 *
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
trait FieldVisitorCtxCtx[DT, SF] extends VisitorCtx[DT, SF] {
  /**
   * Accept the visitor and dispatch the call to the appropriate 'visit' method.
   *
   * @param visitor the visitor to dispatch the call to.
   * @return the ST representation of this context.
   */
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF]
}

/**
 * The visitor context representing a HAPPY Resource definition.
 *
 * @param resourceDefinition the HAPPY resource definition.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class ResourceCtx[DT, SF](resourceDefinition: RuntimeResourceDefinition) extends TypeVisitorCtx[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): DT = {
    visitor.visitResource(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): DT = {
    toCompositeCtx.accept(visitor)
  }

  def toCompositeCtx: CompositeCtx[DT, SF] = {
    CompositeCtx[DT, SF](resourceDefinition)
  }
}

/**
 * The visitor context representing a HAPPY Composite definition.
 *
 * @param compositeDefinition the HAPPY composite definition.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class CompositeCtx[DT, SF](compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends TypeVisitorCtx[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): DT = {
    visitor.visitComposite(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    compositeDefinition
      .getChildren
      .flatMap(toChildCtx(_).accept(visitor))
  }

  def toChildCtx(childDefinition: BaseRuntimeChildDefinition): ChildCtx[DT, SF] = {
    ChildCtx[DT, SF](childDefinition, compositeDefinition)
  }
}

/**
 * The visitor context representing a HAPPY Child definition.
 *
 * @param childDefinition     the happy child definition.
 * @param compositeDefinition the HAPPY composite definition for this child.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class ChildCtx[DT, SF](childDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldVisitorCtxCtx[DT, SF] {
  override def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChild(this)
  }

  def proceed(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    childDefinition match {
      case _: RuntimeChildContainedResources => Nil
      // we need to handle extension before choice as it is of this type but requires
      // the same handling as element child
      case _: RuntimeChildExtension =>
        ElementChildCtx(childDefinition, compositeDefinition).accept(visitor)
      case choiceDefinition: RuntimeChildChoiceDefinition =>
        ChoiceChildCtx(choiceDefinition, compositeDefinition).accept(visitor)
      case _ =>
        ElementChildCtx(childDefinition, compositeDefinition).accept(visitor)
    }
  }
}

/**
 * The visitor context representing a HAPPY Child definition with a single element.
 *
 * @param elementChildDefinition the happy child definition.
 * @param compositeDefinition    the HAPPY composite definition for this child.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class ElementChildCtx[DT, SF](elementChildDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldVisitorCtxCtx[DT, SF] {

  override def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitElementChild(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    val elementName = elementChildDefinition.getElementName
    ElementCtx(elementName, elementChildDefinition, compositeDefinition).accept(visitor)
  }
}

/**
 * The visitor context representing a HAPPY Choice child definition.
 *
 * @param choiceChildDefinition the happy choice child definition.
 * @param compositeDefinition   the HAPPY composite definition for this child.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class ChoiceChildCtx[DT, SF](choiceChildDefinition: RuntimeChildChoiceDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldVisitorCtxCtx[DT, SF] {
  def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitChoiceChild(this)
  }

  def visitChildren(visitor: SchemaVisitor[DT, SF]): Seq[Seq[SF]] = {

    assert(isSingular(choiceChildDefinition), "Collections of choice elements are not supported")
    getOrderedListOfChoiceChildNames(choiceChildDefinition)
      .map(childName => ElementCtx(childName, choiceChildDefinition, compositeDefinition).accept(visitor))
  }
}

/**
 * The visitor context representing a HAPPY element definition.
 *
 * @param elementName         the name of the element.
 * @param childDefinition     the happy child definition.
 * @param compositeDefinition the HAPPY composite definition for this child.
 * @tparam DT @see [[SchemaVisitor]]
 * @tparam SF @see [[SchemaVisitor]]
 */
case class ElementCtx[DT, SF](elementName: String, childDefinition: BaseRuntimeChildDefinition, compositeDefinition: BaseRuntimeElementCompositeDefinition[_]) extends FieldVisitorCtxCtx[DT, SF] {

  /**
   * The definition of the element.
   */
  lazy val elementDefinition: BaseRuntimeElementDefinition[_] = childDefinition.getChildByName(elementName)

  override def accept(visitor: SchemaVisitor[DT, SF]): Seq[SF] = {
    visitor.visitElement(this)
  }
}
