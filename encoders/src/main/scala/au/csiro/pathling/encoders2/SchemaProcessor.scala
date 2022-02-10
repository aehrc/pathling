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
import ca.uhn.fhir.context._


/**
 * A specialized [[SchemaVisitor]] for building schema encoders and converters.
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
trait SchemaProcessor[DT, SF] extends SchemaVisitor[DT, SF] {

  /**
   * Builds a representation for an child element with resolved name.
   *
   * @param childDefinition   the HAPI child definition.
   * @param elementDefinition the HAPI element definition.
   * @param elementName       the element name.
   * @return the representation of of the named child element.
   */
  def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[SF]

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

  /**
   * Gets the max nesting that recursive data types should be expanded to.
   * Zero indicates that fields ot type T should not be expanded in the composite of type T.
   *
   * @return the max nesting that recursive data types should be expanded to
   */
  def maxNestingLevel: Int

  def proceedCompositeChildren(value: CompositeCtx[DT, SF]): Seq[SF] = {
    value.visitChildren(this)
  }

  def compositeBuilder(compositeDefinition: BaseRuntimeElementCompositeDefinition[_]): DT = {
    CompositeCtx(compositeDefinition).accept(this)
  }

  override def visitComposite(compositeCtx: CompositeCtx[DT, SF]): DT = {
    // TODO: Not sure if should be here on in the traversal itself
    EncodingContext.withDefinition(compositeCtx.compositeDefinition) {
      aggregateComposite(compositeCtx, proceedCompositeChildren(compositeCtx))
    }
  }

  override def visitElementChild(elementChildCtx: ElementChildCtx[DT, SF]): Seq[SF] = {
    elementChildCtx.elementChildDefinition match {
      case _: RuntimeChildExtension => Nil
      case _ => super.visitElementChild(elementChildCtx)
    }
  }

  override def visitElement(elementCtx: ElementCtx[DT, SF]): Seq[SF] = {
    if (EncodingContext.currentNestingLevel(elementCtx.elementDefinition) <= maxNestingLevel) {
      buildValue(elementCtx.childDefinition, elementCtx.elementDefinition, elementCtx.elementName)
    } else {
      Nil
    }
  }

  override def aggregateComposite(compositeCtx: CompositeCtx[DT, SF], sfs: Seq[SF]): DT = {
    buildComposite(compositeCtx.compositeDefinition, sfs)
  }

  override def combineChoiceElements(ctx: ChoiceChildCtx[DT, SF], seq: Seq[Seq[SF]]): Seq[SF] = {
    combineChoiceOptions(ctx.choiceChildDefinition, seq)
  }

  override def visitChild(childCtx: ChildCtx[DT, SF]): Seq[SF] = {
    // Inject filtering
    if (shouldExpandChild(childCtx.compositeDefinition, childCtx.childDefinition)) {
      super.visitChild(childCtx)
    } else {
      Nil
    }
  }
}

