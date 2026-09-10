/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 */
package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.DeserializerBuilderProcessor.setterFor
import au.csiro.pathling.encoders.EncoderUtils.arrayExpression
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.schema.SchemaVisitor.isCollection
import au.csiro.pathling.schema.{ElementChildCtx, ElementCtx, SchemaVisitor}
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetStructField, If, IsNotNull, IsNull, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, ObjectType, StructType}
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.jdk.CollectionConverters._

/**
 * The schema processor for building deserializer expressions.
 *
 * @param path            the starting (root) path.
 * @param schemaConverter the schema converter to use.
 * @param parent          the processor for the parent composite.
 */
private[encoders] sealed class DeserializerBuilderProcessor(val path: Option[Expression],
                                                            schemaConverter: SchemaConverter,
                                                            parent: Option[DeserializerBuilderProcessor] = None)
  extends SchemaProcessorWithTypeMappings[Expression, ExpressionWithName] with Deserializer {

  override def fhirContext: FhirContext = schemaConverter.fhirContext

  override def dataTypeMappings: DataTypeMappings = schemaConverter.dataTypeMappings

  override def config: EncoderSettings = schemaConverter.config

  override def combineChoiceOptions(choiceDefinition: RuntimeChildChoiceDefinition,
                                    optionValues: Seq[Seq[ExpressionWithName]]): Seq[ExpressionWithName] = {
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

  override def buildEnumPrimitive(enumDefinition: RuntimePrimitiveDatatypeDefinition,
                                  enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): Expression = {
    // Only apply the special case for non list
    if (enumChildDefinition.getMax == 1) {
      enumerationToDeserializer(enumChildDefinition, path)
    } else {
      super.buildEnumPrimitive(enumDefinition, enumChildDefinition)
    }
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition,
                          elementDefinition: BaseRuntimeElementDefinition[_],
                          elementName: String): Seq[ExpressionWithName] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // NOTE: We need to pass the parent's addToPath to custom encoder, so that it can
    // access multiple elements of the parent composite
    customEncoder.map(_.customDeserializer(parent.get.addToPath, isCollection(childDefinition)))
      .getOrElse(childDefinition match {
        case _: RuntimeChildExtension => Nil
        case _ => super.buildValue(childDefinition, elementDefinition, elementName)
      })
  }

  private def proceedElement(ctx: ElementCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElement(ctx)
  }

  override def visitElement(elementCtx: ElementCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    elementCtx.childDefinition match {
      case _: RuntimeChildExtension => super.visitElement(elementCtx)
      case _: RuntimeChildChoiceDefinition => expandWithName(elementCtx.elementName)
        .proceedElement(elementCtx)
      case _ => super.visitElement(elementCtx)
    }
  }

  private def proceedElementChild(value: ElementChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    super.visitElementChild(value)
  }

  override def visitElementChild(elementChildCtx: ElementChildCtx[Expression, (String, Expression)]): Seq[(String, Expression)] = {
    expandWithName(elementChildCtx.elementChildDefinition.getElementName)
      .proceedElementChild(elementChildCtx)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition,
                               elementDefinition: BaseRuntimeElementDefinition[_],
                               elementName: String): Expression = {

    assert(path.isDefined, "We expect a non-empty path here")
    // The encoder's element type is no longer what the field accesses are bound to, but it is still
    // needed to know which fields the encoder is going to ask for.
    val encoderElementType: DataType = getSqlDatatypeFor(elementDefinition)

    def elementMapper(expression: Expression): Expression = {
      withPath(Some(withEncoderFields(expression, encoderElementType)))
        .buildSimpleValue(childDefinition, elementDefinition, elementName)
    }

    // The element type must come from the data rather than from the encoder's own schema.
    // MapObjects binds its lambda variable to whatever type it is given, so supplying the
    // encoder's type pins every field access inside the lambda to the encoder's field positions
    // and then applies them to rows of a different shape - reading at the wrong offsets whenever
    // the stored order differs, which is what a mergeSchema migration or a change of open types
    // produces. UnresolvedMapObjects defers the type to the analyser, which takes it from the
    // resolved input and rewrites the nested UnresolvedExtractValue nodes by name using the
    // session resolver. The analyser applies the element mapper after the traversal has finished,
    // so the nesting levels it needs have to be carried across with it.
    val array = Invoke(
      UnresolvedMapObjects(EncodingContext.deferring(elementMapper), path.get),
      "array",
      ObjectType(classOf[Array[Any]]))
    arrayExpression(array)
  }

  override def buildElement(elementName: String, elementValue: Expression,
                            definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
    (elementName, elementValue)
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, path)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {

    //noinspection DuplicatedCode
    def getPath: Expression = path
      .getOrElse(GetColumnByOrdinal(0, ObjectType(xhtmlHl7Org.getImplementingClass)))

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

  private def deserializeExtensions(definition: BaseRuntimeElementCompositeDefinition[_],
                                    beanExpression: Expression): Expression = {
    val beanWithFid = RegisterFid(beanExpression, addToPath(ExtensionSupport.FID_FIELD_NAME))
    definition match {
      case _: RuntimeResourceDefinition =>
        val deserializeExtension = UnresolvedCatalystToExternalMap(
          addToPath(ExtensionSupport.EXTENSIONS_FIELD_NAME),
          identity,
          // This function is called outside the scope of the current traversal
          // We need to create a new context for it.
          // This is OK extensions are never nested in this implementation.
          // It also implies that if there were any recursive types allowed as extension values
          // (which there are none ATM) they would be allowed to reach its max depth of nesting
          // in each extension independently regardless of the nesting of the elements they
          // extend (which is the desired behaviour).
          exp => EncodingContext.runWithContext {
            withPath(Some(exp)).buildExtensionValue()
          },
          classOf[Map[Int, ArrayData]]
        )
        AttachExtensions(beanWithFid, deserializeExtension)
      case _ => beanWithFid
    }
  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_],
                              fields: Seq[(String, Expression)]): Expression = {
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
        definition.getChildren.asScala.find(childDef => childDef.getElementName == name).get

      (setterFor(childDefinition), expression)
    }

    val bean: Expression = InitializeJavaBean(compositeInstance, setters.toMap)

    val result = if (supportsExtensions) {
      deserializeExtensions(definition, bean)
    } else {
      bean
    }

    path.map(path =>
      If(IsNull(path), Literal.create(null, ObjectType(definition.getImplementingClass)), result))
      .getOrElse(result)
  }

  /**
   * Supplies, as typed nulls, the fields of a collection element that the encoder expects but the
   * data does not carry.
   *
   * The data is authoritative about its own shape, so where the two disagree on which fields exist,
   * the intersection is what can be represented. Resolving an absent field by name would otherwise
   * fail the analysis, which would leave a table written with more open types configured than the
   * reader has unreadable rather than partially readable.
   *
   * Only the element struct itself and the singular structs within it are rebuilt. A collection
   * nested inside the element conforms its own elements, through the same path as this one, and its
   * field accesses resolve against whatever this leaves in place for it.
   *
   * The element is returned unchanged when the data carries every field the encoder asks for, which
   * is the aligned case, so nothing is added to the work done per row there.
   */
  private def withEncoderFields(element: Expression, encoderType: DataType): Expression = {
    (element.dataType, encoderType) match {
      case (dataStruct: StructType, encoderStruct: StructType)
        if missingAnyField(dataStruct, encoderStruct) =>
        val resolver = SQLConf.get.resolver
        val conformedFields = encoderStruct.fields.toSeq.flatMap { encoderField =>
          dataStruct.fields.indexWhere(field => resolver(field.name, encoderField.name)) match {
            case -1 => Seq(Literal(encoderField.name),
              Literal.create(null, encoderField.dataType))
            case ordinal =>
              val dataField = dataStruct(ordinal)
              Seq(Literal(dataField.name),
                withEncoderFields(GetStructField(element, ordinal, Some(dataField.name)),
                  encoderField.dataType))
          }
        }
        val conformed = CreateNamedStruct(conformedFields)
        // A struct built field by field is never null, so the null case has to be restored: an
        // absent element must stay absent rather than decoding as an empty composite.
        If(IsNull(element), Literal.create(null, conformed.dataType), conformed)
      case _ => element
    }
  }

  /**
   * Returns true if the data does not carry every field the encoder expects, at this level or
   * within any singular struct below it.
   */
  private def missingAnyField(dataType: DataType, encoderType: DataType): Boolean = {
    (dataType, encoderType) match {
      case (dataStruct: StructType, encoderStruct: StructType) =>
        val resolver = SQLConf.get.resolver
        encoderStruct.fields.exists { encoderField =>
          dataStruct.fields.find(field => resolver(field.name, encoderField.name)) match {
            case None => true
            case Some(dataField) => missingAnyField(dataField.dataType, encoderField.dataType)
          }
        }
      case _ => false
    }
  }

  def getSqlDatatypeFor(elementDefinition: BaseRuntimeElementDefinition[_]): DataType = {
    elementDefinition match {
      case compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_] => schemaConverter
        .compositeSchema(compositeElementDefinition) // convert schema
      case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings
        .primitiveToDataType(primitive)
    }
  }

  private def withPath(path: Option[Expression]): DeserializerBuilderProcessor = {
    new DeserializerBuilderProcessor(path, schemaConverter, Some(this))
  }

  private def expandWithName(name: String): DeserializerBuilderProcessor = {
    withPath(Some(addToPath(name)))
  }

  private def addToPath(name: String): Expression = {
    path.map(UnresolvedExtractValue(_, expressions.Literal(name)))
      .getOrElse(UnresolvedAttribute(name))
  }

}

private[encoders] object DeserializerBuilderProcessor extends Deserializer {

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
 * @param schemaConverter the schema converter to use.
 */
class DeserializerBuilder(schemaConverter: SchemaConverter) {
  /**
   * Creates the deserializer expression for given resource class.
   *
   * @param resourceClass the class of a HAPI resource definition.
   * @tparam T the actual type of the resource class.
   * @return the deserializer expression.
   */
  def buildDeserializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    buildDeserializer(schemaConverter.fhirContext.getResourceDefinition(resourceClass))
  }

  /**
   * Creates the deserializer expression for given resource definition.
   *
   * @param resourceDefinition the HAPI resource definition.
   * @return the deserializer expression.
   */
  def buildDeserializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    SchemaVisitor
      .traverseResource(resourceDefinition, new DeserializerBuilderProcessor(None, schemaConverter))
  }
}

/**
 * Companion object for [[DeserializerBuilder]]
 */
object DeserializerBuilder {
  /**
   * Constructs the deserializer builder from a [[SchemaConverter]].
   *
   * @param schemaConverter the schema converter to use.
   * @return the deserializer builder.
   */
  def apply(schemaConverter: SchemaConverter): DeserializerBuilder = {
    new DeserializerBuilder(schemaConverter)
  }
}
