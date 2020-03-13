/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.backports.{InstanceOf, ObjectCast, StaticField}
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import ca.uhn.fhir.model.api.IValueSetEnumBinder
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBase, IBaseDatatype}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream.Empty
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Spark Encoder for FHIR data models.
 */
private[encoders] object EncoderBuilder {

  /**
   * Returns an encoder for the FHIR resource implemented by the given class
   *
   * @param definition The FHIR resource definition
   * @param contained  The FHIR resources to be contained to the given definition
   * @return An ExpressionEncoder for the resource
   */

  def of(definition: BaseRuntimeElementCompositeDefinition[_],
         context: FhirContext,
         mappings: DataTypeMappings,
         converter: SchemaConverter,
         contained: mutable.Buffer[BaseRuntimeElementCompositeDefinition[_]] = mutable.Buffer.empty): ExpressionEncoder[_] = {

    val fhirClass = definition.getImplementingClass

    val schema = converter.parentToStructType(definition, contained)

    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)

    val encoderBuilder = new EncoderBuilder(context,
      mappings,
      converter)

    val serializers = encoderBuilder.serializer(inputObject, definition, contained)

    assert(schema.fields.length == serializers.size,
      "Must have a serializer for each field.")

    val deserializer = encoderBuilder.compositeToDeserializer(definition, None, contained)

    new ExpressionEncoder(
      schema,
      flat = false,
      serializers,
      deserializer = deserializer,
      ClassTag(fhirClass))
  }
}

/**
 * Spark encoder for FHIR resources.
 */
private[encoders] class EncoderBuilder(fhirContext: FhirContext,
                                       dataTypeMappings: DataTypeMappings,
                                       schemaConverter: SchemaConverter) {

  def enumerationToDeserializer(enumeration: RuntimeChildPrimitiveEnumerationDatatypeDefinition,
                                path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    // Get the value and initialize the instance.
    val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    val enumFactory = Class.forName(enumeration.getBoundEnumType.getName + "EnumFactory")

    // Creates a new enum factory instance for each invocation, but this is cheap
    // on modern JVMs and probably more efficient than attempting to pool the underlying
    // FHIR enum factory ourselves.
    val factoryInstance = NewInstance(enumFactory, Nil, false, ObjectType(enumFactory), None)

    Invoke(factoryInstance, "fromCode",
      ObjectType(enumeration.getBoundEnumType),
      List(utfToString))
  }

  /**
   * Converts a FHIR DataType to a UTF8 string usable by Spark.
   */
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

  /**
   * Returns the setter for the given field name.s
   */
  private def setterFor(field: BaseRuntimeChildDefinition): String = {

    // Primitive single-value types typically use the Element suffix in their
    // setters, with the exception of the "div" field for reasons that are not clear.
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

  /**
   * Returns the object type of the given child
   */
  private def objectTypeFor(field: BaseRuntimeChildDefinition): ObjectType = {

    val cls = field match {

      case resource: RuntimeChildResourceDefinition =>
        resource.getChildByName(resource.getElementName).getImplementingClass

      case block: RuntimeChildResourceBlockDefinition =>
        block.getSingleChildOrThrow.getImplementingClass

      case composite: RuntimeChildCompositeDatatypeDefinition =>
        composite.getDatatype

      case primitive: RuntimeChildPrimitiveDatatypeDefinition =>

        primitive.getSingleChildOrThrow.getChildType match {

          case ChildTypeEnum.PRIMITIVE_DATATYPE =>
            primitive.getSingleChildOrThrow.getImplementingClass

          case ChildTypeEnum.PRIMITIVE_XHTML_HL7ORG =>
            classOf[XhtmlNode]

          case ChildTypeEnum.ID_DATATYPE =>
            primitive.getSingleChildOrThrow.getImplementingClass

          case unsupported =>
            throw new IllegalArgumentException("Unsupported child primitive type: " + unsupported)
        }
    }

    ObjectType(cls)
  }

  /**
   * Returns a sequence of name, value expressions
   */
  private def childToExpr(parentObject: Expression,
                          childDefinition: BaseRuntimeChildDefinition): Seq[Expression] = {

    // Contained resources and extensions not yet supported.
    if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Empty

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val getterFunc = if (childDefinition.getElementName.equals("class")) {
        "get" + childDefinition.getElementName.capitalize + "_"
      } else {
        "get" + childDefinition.getElementName.capitalize
      }

      // At this point we don't the actual type of the child, so get it as the general IBaseDatatype
      val choiceObject = Invoke(parentObject,
        getterFunc,
        ObjectType(classOf[IBaseDatatype]))

      val choice = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      // Flatmap to create a list of (name, expression) items.
      // Note that we iterate by types and then look up the name, since this
      // ensures we get the preferred field name for the given type.
      val namedExpressions = choice.getValidChildTypes.toList.flatMap(fhirChildType => {

        val childName = choice.getChildNameByDatatype(fhirChildType)

        val choiceChildDefinition = choice.getChildByName(childName)

        val childObject = If(InstanceOf(choiceObject, choiceChildDefinition.getImplementingClass),
          ObjectCast(choiceObject, ObjectType(choiceChildDefinition.getImplementingClass)),
          Literal.create(null, ObjectType(choiceChildDefinition.getImplementingClass)))

        val childExpr = choiceChildDefinition match {

          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToExpr(childObject, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveEncoderExpression(childObject, primitive);
        }

        List(Literal(childName), childExpr)
      })

      namedExpressions

    } else {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)

      if (childDefinition.getMax != 1) {

        val childList = Invoke(parentObject,
          accessorFor(childDefinition),
          ObjectType(classOf[java.util.List[_]]))

        val elementExpr = definition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => (elem: Expression) => compositeToExpr(elem, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => (elem: Expression) => dataTypeMappings.primitiveEncoderExpression(elem, primitive)
        }

        val childExpr = MapObjects(elementExpr,
          childList,
          objectTypeFor(childDefinition))

        List(Literal(childDefinition.getElementName), childExpr)

      } else {

        // Get the field accessor
        val childObject = Invoke(parentObject,
          accessorFor(childDefinition),
          objectTypeFor(childDefinition))

        val childExpr = definition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToExpr(childObject, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveEncoderExpression(childObject, primitive);
          case narrative: RuntimePrimitiveDatatypeNarrativeDefinition => dataTypeToUtf8Expr(childObject);
          case htmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => dataTypeToUtf8Expr(childObject)
        }

        List(Literal(childDefinition.getElementName), childExpr)
      }
    }
  }

  private def compositeToExpr(inputObject: Expression,
                              definition: BaseRuntimeElementCompositeDefinition[_]): Expression = {

    // Handle references as special cases, since they include a recursive structure
    // that can't be mapped onto a dataframe
    val fields = dataTypeMappings.overrideCompositeExpression(inputObject, definition) match {

      case Some(fields) => fields;
      case None => {

        // Map to (name, value, name, value) expressions for child elements.
        definition.getChildren
          .filter(child => !dataTypeMappings.skipField(definition, child))
          .flatMap(child => childToExpr(inputObject, child))
      }
    }

    val createStruct = CreateNamedStruct(fields)

    expressions.If(IsNull(inputObject),
      Literal.create(null, createStruct.dataType),
      createStruct)
  }

  private def serializer(inputObject: Expression,
                         definition: BaseRuntimeElementCompositeDefinition[_],
                         contained: Seq[BaseRuntimeElementCompositeDefinition[_]]):
  Seq[Expression] = {

    // Map to (name, value, name, value) expressions for child elements.
    val childFields: Seq[Expression] =
      definition.getChildren
        .flatMap(child => childToExpr(inputObject, child))

    // Map to (name, value, name, value) expressions for all contained resources.
    val containedChildFields = contained.flatMap { containedDefinition =>

      val containedChild = GetClassFromContained(inputObject,
        containedDefinition.getImplementingClass)

      Literal(containedDefinition.getName) ::
        CreateNamedStruct(containedDefinition.getChildren
          .flatMap(child => childToExpr(containedChild, child))) ::
        Nil
    }

    // Create a 'contained' struct having the contained elements if declared for the parent.
    val containedChildren = if (contained.nonEmpty) {
      Literal("contained") :: CreateNamedStruct(containedChildFields) :: Nil
    } else {
      Nil
    }

    // The fields are (name, expr) tuples, so just get the expressions for the top level.
    (childFields ++ containedChildren).grouped(2)
      .map(group => group.get(1))
      .toList
  }

  private def listToDeserializer(definition: BaseRuntimeElementDefinition[_ <: IBase],
                                 path: Expression): Expression = {

    val array = definition match {

      case composite: BaseRuntimeElementCompositeDefinition[_] => {

        val elementType = schemaConverter.compositeToStructType(composite)

        Invoke(
          MapObjects(element =>
            compositeToDeserializer(composite, Some(element)),
            path,
            elementType),
          "array",
          ObjectType(classOf[Array[Any]]))
      }

      case primitive: RuntimePrimitiveDatatypeDefinition => {
        val elementType = dataTypeMappings.primitiveToDataType(primitive)

        Invoke(
          MapObjects(element =>
            dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, Some(element)),
            path,
            elementType),
          "array",
          ObjectType(classOf[Array[Any]]))
      }
    }

    StaticInvoke(
      classOf[java.util.Arrays],
      ObjectType(classOf[java.util.List[_]]),
      "asList",
      array :: Nil)
  }

  /**
   * Returns a deserializer for the choice, which will return the first
   * non-null field included in the choice definition. Note this method
   * is based on choice types rather than names, since this will ensure
   * the underlying FHIR API uses the preferred name for the given type.
   *
   * @param fhirChildTypes        the choice's types to deserialize
   * @param choiceChildDefinition the choice definition
   * @param path                  path to the field
   * @return a deserializer expression.
   */
  private def choiceToDeserializer(fhirChildTypes: Seq[Class[_ <: IBase]],
                                   choiceChildDefinition: RuntimeChildChoiceDefinition,
                                   path: Option[Expression]): Expression = {

    if (fhirChildTypes.isEmpty) {

      // No remaining choices, so return null.
      Literal.create(null, ObjectType(dataTypeMappings.baseType()))

    } else {

      def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

      def addToPath(part: String): Expression = path
        .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
        .getOrElse(UnresolvedAttribute(part))

      val fhirChildType = fhirChildTypes.head
      val childName = choiceChildDefinition.getChildNameByDatatype(fhirChildType)

      val choiceField = choiceChildDefinition.getChildByName(childName)
      val childPath = addToPath(childName)

      val deserializer = choiceField match {

        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToDeserializer(composite, Some(childPath))
        case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, Some(childPath))
      }

      val child = ObjectCast(deserializer, ObjectType(dataTypeMappings.baseType()))

      // If this item is not null, deserialize it. Otherwise attempt other choices.
      expressions.If(IsNotNull(childPath),
        child,
        choiceToDeserializer(fhirChildTypes.tail, choiceChildDefinition, path))
    }
  }

  private def childToDeserializer(childDefinition: BaseRuntimeChildDefinition,
                                  path: Option[Expression]): Map[String, Expression] = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    // Contained resources and extensions not yet supported.
    if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Map()

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val choiceChildDefinition = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      Map(childDefinition.getElementName ->
        choiceToDeserializer(choiceChildDefinition.getValidChildTypes.toList,
          choiceChildDefinition,
          path))

    } else if (childDefinition.getMax != 1) {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)
        .asInstanceOf[BaseRuntimeElementDefinition[_ <: IBase]]

      // Handle lists
      Map(childDefinition.getElementName -> listToDeserializer(definition, addToPath(childDefinition.getElementName)))

    } else {

      val childPath = Some(addToPath(childDefinition.getElementName))

      // These must match on the RuntimeChild* structures rather than the definitions,
      // since only the RuntimeChild* structures include default values to be passed
      // to constructors when deserializing some bound objects.
      val result = childDefinition match {

        case boundCode: RuntimeChildPrimitiveBoundCodeDatatypeDefinition =>
          boundCodeToDeserializer(boundCode, childPath)

        case enumeration: RuntimeChildPrimitiveEnumerationDatatypeDefinition =>
          enumerationToDeserializer(enumeration, childPath)

        // Handle bound codeable concepts
        case boundComposite: RuntimeChildCompositeBoundDatatypeDefinition =>
          boundCodeableConceptToDeserializer(boundComposite, childPath)

        case resource: RuntimeChildResourceDefinition =>
          compositeToDeserializer(
            resource.getChildByName(resource.getElementName)
              .asInstanceOf[BaseRuntimeElementCompositeDefinition[_]],
            childPath)

        case block: RuntimeChildResourceBlockDefinition =>
          compositeToDeserializer(block.getSingleChildOrThrow.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], childPath)

        case composite: RuntimeChildCompositeDatatypeDefinition =>
          compositeToDeserializer(composite.getSingleChildOrThrow.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], childPath)

        case primitive: RuntimeChildPrimitiveDatatypeDefinition => {

          val definition = childDefinition.getChildByName(childDefinition.getElementName)

          definition match {
            case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, childPath)
            case htmlHl7: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => xhtmlHl7ToDeserializer(htmlHl7, childPath)
          }
        }
      }

      // Item is not a list,
      // nonListChildToDeserializer(childDefinition, path)

      Map(childDefinition.getElementName -> result)
    }

  }

  private def boundCodeToDeserializer(boundCode: RuntimeChildPrimitiveBoundCodeDatatypeDefinition,
                                      path: Option[Expression]): Expression = {

    // Construct a bound code instance with the value set based on the enumeration.
    val boundCodeInstance = NewInstance(boundCode.getDatatype,
      StaticField(boundCode.getBoundEnumType,
        ObjectType(classOf[IValueSetEnumBinder[_]]),
        "VALUESET_BINDER") :: Nil,
      ObjectType(boundCode.getDatatype))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    // Get the value and initialize the instance.
    val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    InitializeJavaBean(boundCodeInstance, Map("setValueAsString" -> utfToString))
  }

  private def boundCodeableConceptToDeserializer(boundCodeable: RuntimeChildCompositeBoundDatatypeDefinition,
                                                 path: Option[Expression]): Expression = {

    val boundCodeInstance = NewInstance(boundCodeable.getDatatype,
      StaticField(boundCodeable.getBoundEnumType,
        ObjectType(classOf[IValueSetEnumBinder[_]]),
        "VALUESET_BINDER") :: Nil,
      ObjectType(boundCodeable.getDatatype))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))


    // Temporarily treat as a composite.
    val definition = boundCodeable.getChildByName(boundCodeable.getElementName)

    compositeToDeserializer(definition.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], path)
  }

  private def xhtmlHl7ToDeserializer(xhtmlHl7: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition, path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(xhtmlHl7.getImplementingClass)))

    val newInstance = NewInstance(xhtmlHl7.getImplementingClass,
      Nil,
      ObjectType(xhtmlHl7.getImplementingClass))

    val stringValue = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    // If there is a non-null value, create and set the xhtml node.
    expressions.If(IsNull(stringValue),
      expressions.Literal.create(null, newInstance.dataType),
      InitializeJavaBean(newInstance, Map("setValueAsString" ->
        stringValue)))
  }

  /**
   * Returns an expression for deserializing a composite structure at the given path along with
   * any contained resources declared against the structure.
   */
  private def compositeToDeserializer(definition: BaseRuntimeElementCompositeDefinition[_],
                                      path: Option[Expression],
                                      contained: Seq[BaseRuntimeElementCompositeDefinition[_]] = Nil): Expression = {

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0,
      schemaConverter.compositeToStructType(definition)))

    // Map to (name, value, name, value) expressions for child elements.
    val childExpressions: Map[String, Expression] = definition.getChildren
      .filter(child => !dataTypeMappings.skipField(definition, child))
      .flatMap(child => childToDeserializer(child, path)).toMap

    val compositeInstance = NewInstance(definition.getImplementingClass,
      Nil,
      ObjectType(definition.getImplementingClass))

    val setters = childExpressions.map { case (name, expression) =>

      // Option types are not visible in the getChildByName, so we fall back
      // to looking for them in the child list.
      val childDefinition = if (definition.getChildByName(name) != null)
        definition.getChildByName(name)
      else
        definition.getChildren.find(childDef => childDef.getElementName == name).get

      (setterFor(childDefinition), expression)
    }

    val bean: Expression = InitializeJavaBean(compositeInstance, setters)

    // Deserialize any Contained resources to the new Object through successive calls
    // to 'addContained'.
    val result = contained.foldLeft(bean)((value, containedResource) => {

      Invoke(value,
        "addContained",
        ObjectType(definition.getImplementingClass),
        compositeToDeserializer(containedResource,
          Some(UnresolvedAttribute("contained." + containedResource.getName))) :: Nil)
    })

    if (path.nonEmpty) {
      expressions.If(
        IsNull(getPath),
        expressions.Literal.create(null, ObjectType(definition.getImplementingClass)),
        result)
    } else {
      result
    }
  }
}

/**
 * An Expression extracting an object having the given class definition from a List of FHIR
 * Resources.
 */
case class GetClassFromContained(targetObject: Expression,
                                 containedClass: Class[_])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = targetObject.nullable

  override def children: Seq[Expression] = targetObject :: Nil

  override def dataType: DataType = ObjectType(containedClass)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val javaType = containedClass.getName
    val obj = targetObject.genCode(ctx)

    ev.copy(code =
      code"""
            |${obj.code}
            |$javaType ${ev.value} = null;
            |boolean ${ev.isNull} = true;
            |java.util.List<Object> contained = ${obj.value}.getContained();
            |
            |for (int containedIndex = 0; containedIndex < contained.size(); containedIndex++) {
            |  if (contained.get(containedIndex) instanceof $javaType) {
            |    ${ev.value} = ($javaType) contained.get(containedIndex);
            |    ${ev.isNull} = false;
            |  }
            |}
       """.stripMargin)
  }
}
