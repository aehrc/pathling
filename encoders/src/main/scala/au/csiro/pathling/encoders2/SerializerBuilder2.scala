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

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders.{InstanceOf, ObjectCast, SchemaConfig}
import au.csiro.pathling.encoders2.ExtensionSupport.{EXTENSIONS_FIELD_NAME, FID_FIELD_NAME}
import au.csiro.pathling.encoders2.SchemaVisitor.isCollection
import au.csiro.pathling.encoders2.SerializerBuilderProcessor.{dataTypeToUtf8Expr, getChildExpression, objectTypeFor}
import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.expressions.objects.{ExternalMapToCatalyst, Invoke, MapObjects, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, ObjectType}
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBaseDatatype, IBaseHasExtensions, IBaseResource}
import org.hl7.fhir.r4.model.{Base, DomainResource, Extension}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * The schema processor for building serializer expressions.
 *
 * @param expression       the starting (root) expression.
 * @param dataTypeMappings data type mappings to use.
 */
private[encoders2] class SerializerBuilderProcessor(expression: Expression, val dataTypeMappings: DataTypeMappings,
                                                    override val maxNestingLevel: Int, override val expandExtensions: Boolean, override val fhirContext: FhirContext) extends
  SchemaProcessorWithTypeMappings[Expression, ExpressionWithName] {

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[ExpressionWithName] = {
    // add custom encoder
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    val evaluator: (Expression => Expression) => Expression = if (isCollection(childDefinition)) {
      MapObjects(_,
        expression,
        objectTypeFor(childDefinition))
    } else {
      encoder => encoder(expression)
    }
    customEncoder.map(_.customSerializer2(evaluator))
      .getOrElse(super.buildValue(childDefinition, elementDefinition, elementName))
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Expression = {
    MapObjects(withExpression(_).buildSimpleValue(childDefinition, elementDefinition, elementName),
      expression,
      objectTypeFor(childDefinition))
  }

  override def buildElement(elementName: String, elementValue: Expression, definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
    // Named serializer
    (elementName, elementValue)
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    dataTypeMappings.primitiveEncoderExpression(expression, primitive)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {
    dataTypeToUtf8Expr(expression)
  }

  private def createExtensionsFields(definition: BaseRuntimeElementCompositeDefinition[_]): Seq[(String, Expression)] = {
    val maybeExtensionValueField = definition match {
      case _: RuntimeResourceDefinition =>
        val collectExtensionsExpression = StaticInvoke(
          classOf[SerializerBuilderProcessor],
          ObjectType(classOf[Map[Int, java.util.List[Extension]]]),
          "flattenExtensions",
          expression :: Nil
        )
        val mappingExpression = ExternalMapToCatalyst(collectExtensionsExpression, IntegerType, identity, keyNullable = false,
          ObjectType(classOf[java.util.List[Extension]]),
          obj => this.withExpression(obj).buildExtensionValue(),
          valueNullable = false
        )
        (EXTENSIONS_FIELD_NAME, mappingExpression) :: Nil
      case _ => Nil
    }
    // append _fid serializer
    (FID_FIELD_NAME, StaticInvoke(
      classOf[System], IntegerType, "identityHashCode", expression :: Nil)) :: maybeExtensionValueField
  }

  override def proceedCompositeChildren(value: CompositeNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    dataTypeMappings.overrideCompositeExpression2(expression, value.definition).getOrElse(super.proceedCompositeChildren(value))
  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_], fields: Seq[(String, Expression)]): Expression = {

    val allFields = if (expandExtensions) {
      fields ++ createExtensionsFields(definition)
    } else {
      fields
    }

    val struct = CreateNamedStruct(allFields.flatMap({ case (name, serializer) => Seq(Literal(name), serializer) }))
    If(IsNull(expression), Literal.create(null, struct.dataType), struct)
  }

  private def proceedElementChild(value: ElementChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElementChild(value)
  }

  override def visitElementChild(value: ElementChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    withExpression(getChildExpression(expression, value.childDefinition)).proceedElementChild(value)
  }

  private def proceedChoiceChild(value: ChoiceChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitChoiceChild(value)
  }

  override def visitChoiceChild(value: ChoiceChildNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    withExpression(getChildExpression(expression, value.choiceDefinition, ObjectType(classOf[IBaseDatatype]))).proceedChoiceChild(value)
  }

  private def proceedElement(ctx: ElementNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElement(ctx)
  }

  override def visitElement(ctx: ElementNode[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    ctx.childDefinition match {
      case _: RuntimeChildExtension => super.visitElement(ctx)
      case _: RuntimeChildChoiceDefinition =>
        val implementingClass = ctx.elementDefinition.getImplementingClass
        val optionExpression = If(InstanceOf(expression, implementingClass),
          ObjectCast(expression, ObjectType(implementingClass)),
          Literal.create(null, ObjectType(implementingClass)))
        withExpression(optionExpression).proceedElement(ctx)
      case _ => super.visitElement(ctx)
    }
  }

  private def withExpression(expression: Expression): SerializerBuilderProcessor = {
    new SerializerBuilderProcessor(expression, dataTypeMappings, maxNestingLevel, expandExtensions, fhirContext)
  }
}

private[encoders2] object SerializerBuilderProcessor {

  private def getChildExpression(parentObject: Expression,
                                 childDefinition: BaseRuntimeChildDefinition, dataType: DataType): Expression = {
    Invoke(parentObject,
      accessorFor(childDefinition),
      dataType)
  }

  private def getChildExpression(parentObject: Expression,
                                 childDefinition: BaseRuntimeChildDefinition): Expression = {

    // get the child object type
    // for children with cardinality of MANY this is java.util.List
    val childObjectType = if (childDefinition.getMax != 1) {
      ObjectType(classOf[java.util.List[_]])
    } else {
      objectTypeFor(childDefinition)
    }
    getChildExpression(parentObject, childDefinition, childObjectType)
  }

  private def dataTypeToUtf8Expr(inputObject: Expression): Expression = {
    StaticInvoke(
      classOf[UTF8String],
      DataTypes.StringType,
      "fromString",
      List(Invoke(inputObject,
        "getValueAsString",
        ObjectType(classOf[String]))))
  }

  /**
   * Returns the accessor method for the given child field.
   */
  private def accessorFor(field: BaseRuntimeChildDefinition): String = {

    // Primitive single-value types typically use the Element suffix in their
    // accessors, with the exception of the "div" field for reasons that are not clear.
    //noinspection DuplicatedCode
    if (field.isInstanceOf[RuntimeChildPrimitiveDatatypeDefinition] &&
      field.getMax == 1 &&
      field.getElementName != "div")
      "get" + field.getElementName.capitalize + "Element"
    else {
      if (field.getElementName.equals("class")) {
        "get" + field.getElementName.capitalize + "_"
      } else {
        "get" + field.getElementName.capitalize
      }
    }
  }

  private def getSingleChild(childDefinition: BaseRuntimeDeclaredChildDefinition) = {
    childDefinition.getChildByName(childDefinition.getValidChildNames.iterator.next)
  }

  /**
   * Returns the object type of the given child
   */
  private def objectTypeFor(field: BaseRuntimeChildDefinition): ObjectType = {

    //noinspection DuplicatedCode
    val cls = field match {

      case resource: RuntimeChildResourceDefinition =>
        resource.getChildByName(resource.getElementName).getImplementingClass

      case block: RuntimeChildResourceBlockDefinition =>
        getSingleChild(block).getImplementingClass

      case _: RuntimeChildExtension => classOf[Extension]

      case composite: RuntimeChildCompositeDatatypeDefinition =>
        composite.getDatatype

      case primitive: RuntimeChildPrimitiveDatatypeDefinition =>
        getSingleChild(primitive).getChildType match {
          case ChildTypeEnum.PRIMITIVE_DATATYPE =>
            getSingleChild(primitive).getImplementingClass

          case ChildTypeEnum.PRIMITIVE_XHTML_HL7ORG =>
            classOf[XhtmlNode]

          case ChildTypeEnum.ID_DATATYPE =>
            getSingleChild(primitive).getImplementingClass

          case unsupported =>
            throw new IllegalArgumentException("Unsupported child primitive type: " + unsupported)
        }
    }
    ObjectType(cls)
  }


  def flattenExtensions(resource: DomainResource): Map[Int, java.util.List[Extension]] = {
    def flattenBase(obj: Base): List[(Int, java.util.List[Extension])] = {

      val childrenExts = resource.children()
        .map(p => obj.getProperty(p.getName.hashCode, p.getName, false))
        .filter(_ != null)
        .flatMap(_.flatMap(flattenBase))
        .toList

      obj match {
        // TODO: fix the type cast
        case hasExt: IBaseHasExtensions if hasExt.hasExtension =>
          (System.identityHashCode(obj), hasExt.getExtension.asInstanceOf[java.util.List[Extension]]) :: childrenExts
        case _ => childrenExts
      }
    }

    flattenBase(resource).toMap
  }

}

/**
 * The builder of serializer expressions for HAPI representation of FHIR resources.
 *
 * @param fhirContext     the FHIR context to use.
 * @param mappings        the data type mappings to use.
 * @param maxNestingLevel the max nesting level to use.
 */
class SerializerBuilder2(fhirContext: FhirContext, mappings: DataTypeMappings, maxNestingLevel: Int, supportsExtensions: Boolean) {

  /**
   * Creates the serializer expression for given resource definition.
   *
   * @param resourceDefinition the HAPI resource definition.
   * @return the serializer expression.
   */
  def buildSerializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    val fhirClass = resourceDefinition.asInstanceOf[BaseRuntimeElementDefinition[_]].getImplementingClass
    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)

    SchemaVisitor.traverseResource(resourceDefinition, new SerializerBuilderProcessor(inputObject, mappings, maxNestingLevel, supportsExtensions, fhirContext))
  }

  /**
   * Creates the serializer expression for given resource class.
   *
   * @param resourceClass the class of a HAPI resource definition.
   * @tparam T the actual type of the resource class.
   * @return the serializer expression.
   */
  def buildSerializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    buildSerializer(fhirContext.getResourceDefinition(resourceClass))
  }
}

/**
 * Companion object for [[SerializerBuilder2]]
 */
object SerializerBuilder2 {
  /**
   * Constructs a serializer builder from a [[SchemaConfig]].
   *
   * @param config the schema config to use.
   * @return the serializer builder.
   */
  def apply(config: SchemaConfig): SerializerBuilder2 = {
    new SerializerBuilder2(config.fhirContext, config.dataTypeMappings, config.maxNestingLevel, config.supportsExtensions)
  }
}