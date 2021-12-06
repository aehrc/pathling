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

import au.csiro.pathling.encoders.EncoderUtils.arrayExpression
import au.csiro.pathling.encoders._
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.DeserializerBuilderProcessor.setterFor
import au.csiro.pathling.encoders2.SchemaVisitor.isCollection
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNotNull, IsNull, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * The schema processor for building deserializer expressions.
 *
 * @param path             the starting (root) path.
 * @param dataTypeMappings data type mappings to use.
 * @param schemaConverter  the schema converter to use.
 * @param parent           the processor for the parent composite.
 */
private[encoders2] sealed class DeserializerBuilderProcessor(val path: Option[Expression], val dataTypeMappings: DataTypeMappings, schemaConverter: SchemaConverter2,
                                                             parent: Option[DeserializerBuilderProcessor] = None)
  extends SchemaProcessorWithTypeMappings[Expression, ExpressionWithName] with Deserializer {


  override def maxNestingLevel: Int = schemaConverter.maxNestingLevel

  override def expandExtensions: Boolean = schemaConverter.supportsExtensions

  override def fhirContext: FhirContext = schemaConverter.fhirContext

  override def combineChoiceOptions(choiceDefinition: RuntimeChildChoiceDefinition, optionValues: Seq[Seq[ExpressionWithName]]): Seq[ExpressionWithName] = {
    // so how do we aggregate children of a choice
    // we need to fold all the child expression ignoring the name and then
    // create decoder for this choice

    // Create a list of child expressions
    // We expect all of them be either empty of one-element lists.
    // We can drop the empty ones
    val optionExpressions = optionValues
      .filter(_.nonEmpty)
      .map {
        case head :: Nil => head
        case _ => throw new IllegalArgumentException("A single element value expected")
      }
    // Fold child expressions into one
    val nullDeserializer: Expression = Literal.create(null, ObjectType(dataTypeMappings.baseType()))
    // NOTE: Fold right is needed to maintain compatibility with v1 implementation
    val choiceDeserializer = optionExpressions.foldRight(nullDeserializer) {
      case ((optionName, optionDeserializer), composite) =>
        If(IsNotNull(addToPath(optionName)),
          ObjectCast(optionDeserializer, ObjectType(dataTypeMappings.baseType())),
          composite)
    }
    Seq((choiceDefinition.getElementName, choiceDeserializer))
  }

  override def buildEnumPrimitive(enumDefinition: RuntimePrimitiveDatatypeDefinition, enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): Expression = {

    // TODO: [#414] This  seem to be an unnecessary complication as enum types can be decoded
    //  in the same way as other types. Unless there is a substantial performance improvement
    //  this special case should be removed.

    // Only apply the special case for non list
    if (enumChildDefinition.getMax == 1) {
      enumerationToDeserializer(enumChildDefinition, path)
    } else {
      super.buildEnumPrimitive(enumDefinition, enumChildDefinition)
    }
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[ExpressionWithName] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // NOTE: We need to pass the parent's addToPath to custom encoder, so that it can
    // access multiple elements of the parent composite
    customEncoder.map(_.customDeserializer2(parent.get.addToPath, isCollection(childDefinition)))
      .getOrElse(childDefinition match {
        case _: RuntimeChildExtension => Nil // Seq(buildElement("_fid", IntegerType, elementDefinition))
        case _ => super.buildValue(childDefinition, elementDefinition, elementName)
      })
  }

  private def proceedElement(ctx: ElementNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElement(ctx)
  }

  override def visitElement(ctx: ElementNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    ctx.childDefinition match {
      case _: RuntimeChildExtension => super.visitElement(ctx)
      case _: RuntimeChildChoiceDefinition => expandWithName(ctx.elementName).proceedElement(ctx)
      case _ => super.visitElement(ctx)
    }
  }

  private def proceedElementChild(value: ElementChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElementChild(value)
  }

  override def visitElementChild(value: ElementChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    expandWithName(value.childDefinition.getElementName).proceedElementChild(value)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Expression = {

    def elementMapper(expression: Expression): Expression = {
      withPath(Some(expression)).buildSimpleValue(childDefinition, elementDefinition, elementName)
    }

    assert(path.isDefined, "We expect a non-empty path here")
    val elementType: DataType = getSqlDatatypeFor(elementDefinition)
    val array = Invoke(
      MapObjects(elementMapper,
        path.get,
        elementType),
      "array",
      ObjectType(classOf[Array[Any]]))
    arrayExpression(array)
  }

  override def buildElement(elementName: String, elementValue: Expression, definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
    (elementName, elementValue)
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, path)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {

    //noinspection DuplicatedCode
    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(xhtmlHl7Org.getImplementingClass)))

    val newInstance = NewInstance(xhtmlHl7Org.getImplementingClass,
      Nil,
      ObjectType(xhtmlHl7Org.getImplementingClass))

    val stringValue = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    // If there is a non-null value, create and set the xhtml node.
    expressions.If(IsNull(stringValue),
      Literal.create(null, newInstance.dataType),
      InitializeJavaBean(newInstance, Map("setValueAsString" ->
        stringValue)))
  }

  private def deserializeExtensions(definition: BaseRuntimeElementCompositeDefinition[_], beanExpression: Expression): Expression = {
    val beanWithFid = PutFid(beanExpression, addToPath("_fid"))
    definition match {
      case _: RuntimeResourceDefinition =>
        val deserializeExtension = UnresolvedCatalystToExternalMap(
          addToPath("_extension"),
          identity,
          /// TODO: Fix the encoding context creation
          // This function somehow managed to be called ouside the scope of the current traversal
          // Which may actually be OK, but s
          exp => EncodingContext.runWithContext {
            withPath(Some(exp)).buildExtensionValue()
          },
          classOf[Map[Int, ArrayData]]
        )
        AttachExtensions(beanWithFid, deserializeExtension)
      case _ => beanWithFid
    }
  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_], fields: Seq[(String, Expression)]): Expression = {
    //noinspection DuplicatedCode
    val compositeInstance = NewInstance(definition.getImplementingClass,
      Nil,
      ObjectType(definition.getImplementingClass))

    val setters = fields.map { case (name, expression) =>
      // Option types are not visible in the getChildByName, so we fall back
      // to looking for them in the child list.
      val childDefinition = if (definition.getChildByName(name) != null)
        definition.getChildByName(name)
      else
        definition.getChildren.find(childDef => childDef.getElementName == name).get

      (setterFor(childDefinition), expression)
    }

    val bean: Expression = InitializeJavaBean(compositeInstance, setters.toMap)

    val result = if (expandExtensions) {
      deserializeExtensions(definition, bean)
    } else {
      bean
    }

    path.map(path =>
      If(IsNull(path), Literal.create(null, ObjectType(definition.getImplementingClass)), result))
      .getOrElse(result)
  }

  def getSqlDatatypeFor(elementDefinition: BaseRuntimeElementDefinition[_]): DataType = {
    elementDefinition match {
      case compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_] => schemaConverter.compositeSchema(compositeElementDefinition) // convert schema
      case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive)
    }
  }

  private def withPath(path: Option[Expression]): DeserializerBuilderProcessor = {
    new DeserializerBuilderProcessor(path, dataTypeMappings, schemaConverter, Some(this))
  }

  private def expandWithName(name: String): DeserializerBuilderProcessor = {
    withPath(Some(addToPath(name)))
  }

  private def addToPath(name: String): Expression = {
    path.map(UnresolvedExtractValue(_, expressions.Literal(name)))
      .getOrElse(UnresolvedAttribute(name))
  }

}

private[encoders2] object DeserializerBuilderProcessor extends Deserializer {

  /**
   * Returns the setter for the given field name.
   */
  private def setterFor(field: BaseRuntimeChildDefinition): String = {

    // Primitive single-value types typically use the Element suffix in their
    // setters, with the exception of the "div" field for reasons that are not clear.
    //noinspection DuplicatedCode
    if (field.isInstanceOf[RuntimeChildPrimitiveDatatypeDefinition] &&

      // Enumerations are set directly rather than via elements.
      !field.isInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition] &&
      field.getMax == 1 && field.getElementName != "div")
      "set" + field.getElementName.capitalize + "Element"
    else if (field.getElementName.equals("class")) {
      "set" + field.getElementName.capitalize + "_"
    } else {
      "set" + field.getElementName.capitalize
    }
  }

}

/**
 * The builder of deserializer expressions for HAPI representation of FHIR resources.
 *
 * @param fhirContext     the FHIR context to use.
 * @param mappings        the data type mappings to use.
 * @param maxNestingLevel the max nesting level to use.
 * @param schemaConverter the schema converter to use.
 */
class DeserializerBuilder2(fhirContext: FhirContext, mappings: DataTypeMappings, maxNestingLevel: Int,
                           supportsExtensions: Boolean,
                           schemaConverter: SchemaConverter2) {
  /**
   * Creates the deserializer expression for given resource class.
   *
   * @param resourceClass the class of a HAPI resource definition.
   * @tparam T the actual type of the resource class.
   * @return the deserializer expression.
   */
  def buildDeserializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    buildDeserializer(fhirContext.getResourceDefinition(resourceClass))
  }

  /**
   * Creates the deserializer expression for given resource definition.
   *
   * @param resourceDefinition the HAPI resource definition.
   * @return the deserializer expression.
   */
  def buildDeserializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    SchemaVisitor.traverseResource(resourceDefinition, new DeserializerBuilderProcessor(None, mappings, schemaConverter))
  }
}

/**
 * Companion object for [[DeserializerBuilder2]]
 */
object DeserializerBuilder2 {
  /**
   * Constructs the deserializer builder from a [[SchemaConverter2]].
   *
   * @param schemaConverter the schema converter to use.
   * @return the deserializer builder.
   */
  def apply(schemaConverter: SchemaConverter2): DeserializerBuilder2 = {
    new DeserializerBuilder2(schemaConverter.fhirContext, schemaConverter.dataTypeMappings,
      schemaConverter.maxNestingLevel, schemaConverter.supportsExtensions, schemaConverter)
  }
}