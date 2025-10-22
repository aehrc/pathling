/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.ExtensionSupport.{EXTENSIONS_FIELD_NAME, FID_FIELD_NAME}
import au.csiro.pathling.encoders.SerializerBuilderProcessor.{dataTypeToUtf8Expr, getChildExpression, objectTypeFor}
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.schema.SchemaVisitor.isCollection
import au.csiro.pathling.schema._
import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.expressions.objects.{ExternalMapToCatalyst, Invoke, MapObjects}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBaseDatatype, IBaseHasExtensions, IBaseReference, IBaseResource}
import org.hl7.fhir.r4.model.{Base, Extension, Quantity}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

import scala.jdk.CollectionConverters._

/**
 * The schema processor for building serializer expressions.
 *
 * @param expression       the starting (root) expression.
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings the data type mappings to use.
 * @param config           the EncoderSettings to use.
 */
private[encoders] class SerializerBuilderProcessor(expression: Expression,
                                                   override val fhirContext: FhirContext,
                                                   override val dataTypeMappings: DataTypeMappings,
                                                   override val config: EncoderSettings) extends
  SchemaProcessorWithTypeMappings[Expression, ExpressionWithName] {

  override def buildValue(childDefinition: BaseRuntimeChildDefinition,
                          elementDefinition: BaseRuntimeElementDefinition[_],
                          elementName: String): Seq[ExpressionWithName] = {
    // add custom encoder
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    val evaluator: (Expression => Expression) => Expression = if (isCollection(childDefinition)) {
      MapObjects(_,
        expression,
        objectTypeFor(childDefinition))
    } else {
      encoder => encoder(expression)
    }
    customEncoder.map(_.customSerializer(evaluator))
      .getOrElse(super.buildValue(childDefinition, elementDefinition, elementName))
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition,
                               elementDefinition: BaseRuntimeElementDefinition[_],
                               elementName: String): Expression = {
    MapObjects(withExpression(_).buildSimpleValue(childDefinition, elementDefinition, elementName),
      expression,
      objectTypeFor(childDefinition))
  }

  override def buildElement(elementName: String, elementValue: Expression,
                            definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
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
        val collectExtensionsExpression = Catalyst.staticInvoke(
          classOf[SerializerBuilderProcessor],
          ObjectType(classOf[Map[Int, java.util.List[Extension]]]),
          "flattenExtensions",
          expression :: Nil
        )
        val mappingExpression = ExternalMapToCatalyst(collectExtensionsExpression, IntegerType,
          identity, keyNullable = false,
          ObjectType(classOf[java.util.List[Extension]]),
          obj => this.withExpression(obj).buildExtensionValue(),
          valueNullable = false
        )
        (EXTENSIONS_FIELD_NAME, mappingExpression) :: Nil
      case _ => Nil
    }
    // append _fid serializer
    (FID_FIELD_NAME, Catalyst.staticInvoke(
      classOf[System], IntegerType, "identityHashCode",
      expression :: Nil)) :: maybeExtensionValueField
  }

  private def createSyntheticSerializers(value: CompositeCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    value.compositeDefinition.getImplementingClass match {
      case cls if classOf[Quantity].isAssignableFrom(cls) => QuantitySupport
        .createExtraSerializers(expression)
      case _ => Nil
    }
  }

  override def proceedCompositeChildren(value: CompositeCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    dataTypeMappings.overrideCompositeExpression(expression, value.compositeDefinition)
      .getOrElse(super.proceedCompositeChildren(value) ++ createSyntheticSerializers(value))

  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_],
                              fields: Seq[(String, Expression)]): Expression = {

    val allFields: Seq[(String, Expression)] = if (supportsExtensions) {
      fields ++ createExtensionsFields(definition)
    } else {
      fields
    }
    val struct = CreateNamedStruct(
      allFields.flatMap({ case (name, serializer) => Seq(Literal(name), serializer) }))
    If(IsNull(expression), Literal.create(null, struct.dataType), struct)
  }

  private def proceedElementChild(value: ElementChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElementChild(value)
  }

  override def visitElementChild(elementChildCtx: ElementChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    withExpression(getChildExpression(expression, elementChildCtx.elementChildDefinition))
      .proceedElementChild(elementChildCtx)
  }

  private def proceedChoiceChild(value: ChoiceChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitChoiceChild(value)
  }

  override def visitChoiceChild(choiceChildCtx: ChoiceChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    withExpression(getChildExpression(expression, choiceChildCtx.choiceChildDefinition,
      ObjectType(classOf[IBaseDatatype]))).proceedChoiceChild(choiceChildCtx)
  }

  private def proceedElement(ctx: ElementCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElement(ctx)
  }

  override def visitElement(elementCtx: ElementCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    elementCtx.childDefinition match {
      case _: RuntimeChildExtension => super.visitElement(elementCtx)
      case _: RuntimeChildChoiceDefinition =>
        val implementingClass = elementCtx.elementDefinition.getImplementingClass
        val optionExpression = ObjectCast(expression, ObjectType(implementingClass), lenient = true)
        withExpression(optionExpression).proceedElement(elementCtx)
      case _ => super.visitElement(elementCtx)
    }
  }

  private def withExpression(expression: Expression): SerializerBuilderProcessor = {
    new SerializerBuilderProcessor(expression, fhirContext, dataTypeMappings, config)
  }
}

private[encoders] object SerializerBuilderProcessor {

  private def getChildExpression(parentObject: Expression,
                                 childDefinition: BaseRuntimeChildDefinition,
                                 dataType: DataType): Expression = {

    val (hasMethod, getMethod) = accessorsFor(childDefinition)
    GetHapiValue(parentObject, dataType, hasMethod, getMethod)
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
    Catalyst.staticInvoke(
      classOf[UTF8String],
      DataTypes.StringType,
      "fromString",
      List(Invoke(inputObject,
        "getValueAsString",
        ObjectType(classOf[String]))))
  }

  /**
   * Returns the accessor methods for the given child field.
   * These are the 'has' and 'get' methods that test for the presence of the field an retrieve its value.
   *
   * @param field the runtime child definition of the field
   * @return a tuple containing the names of  'has' and 'get' methods
   */
  private def accessorsFor(field: BaseRuntimeChildDefinition): (String, String) = {

    def defaultAccessors(suffix: String): (String, String) = {
      ("has" + suffix, "get" + suffix)
    }

    // Primitive single-value types typically use the Element suffix in their
    // accessors, with the exception of the "div" field for reasons that are not clear.
    //noinspection DuplicatedCode
    field match {
      case p: RuntimeChildPrimitiveDatatypeDefinition if p.getMax == 1 && p
        .getElementName != "div" =>
        if ("reference" == p.getElementName && classOf[IBaseReference]
          .isAssignableFrom(p.getField.getDeclaringClass)) {
          // Special case for subclasses of IBaseReference.
          // The accessor getReferenceElement returns IdType rather than 
          // StringType and getReferenceElement_ needs to be used instead.
          // All subclasses of IBaseReference have a getReferenceElement_ 
          // method.
          ("hasReferenceElement", "getReferenceElement_")
        } else {
          defaultAccessors(p.getElementName.capitalize + "Element")
        }
      case f if f.getElementName.equals("class") =>
        defaultAccessors(f.getElementName.capitalize + "_")
      case _ =>
        defaultAccessors(field.getElementName.capitalize)
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


  def flattenExtensions(composite: Base): Map[Int, java.util.List[Extension]] = {

    def flattenBase(obj: Base): List[(Int, java.util.List[Extension])] = {

      val childrenExts = obj.children().asScala
        .map(p => obj.getProperty(p.getName.hashCode, p.getName, false))
        .filter(_ != null)
        .flatMap(_.flatMap(flattenBase))
        .toList

      obj match {
        case hasExt: IBaseHasExtensions if hasExt.hasExtension =>
          (System.identityHashCode(obj), hasExt.getExtension
            .asInstanceOf[java.util.List[Extension]]) :: childrenExts
        case _ => childrenExts
      }
    }

    flattenBase(composite).toMap
  }

}

/**
 * The builder of serializer expressions for HAPI representation of FHIR resources.
 *
 * @param fhirContext the FHIR context to use.
 * @param mappings    the data type mappings to use.
 * @param config      the EncoderSettings to use.
 */
class SerializerBuilder(fhirContext: FhirContext, mappings: DataTypeMappings,
                        config: EncoderSettings) {

  /**
   * Creates the serializer expression for given resource definition.
   *
   * @param resourceDefinition the HAPI resource definition.
   * @return the serializer expression.
   */
  def buildSerializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    val fhirClass = resourceDefinition.asInstanceOf[BaseRuntimeElementDefinition[_]]
      .getImplementingClass
    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)

    SchemaVisitor.traverseResource(resourceDefinition,
      new SerializerBuilderProcessor(inputObject, fhirContext, mappings, config))
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
 * Companion object for [[SerializerBuilder]]
 */
object SerializerBuilder {
  /**
   * Constructs a serializer builder from a [[EncoderContext]].
   *
   * @param context the schema config to use.
   * @return the serializer builder.
   */
  def apply(context: EncoderContext): SerializerBuilder = {
    new SerializerBuilder(context.fhirContext, context.dataTypeMappings, context.config)
  }
}
