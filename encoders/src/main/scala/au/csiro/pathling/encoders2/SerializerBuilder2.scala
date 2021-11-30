package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders.{InstanceOf, ObjectCast}
import au.csiro.pathling.encoders2.SerializerBuilderVisitor.{dataTypeToUtf8Expr, getChildExpression, objectTypeFor}
import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, MapObjects, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, DataTypes, ObjectType}
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBaseDatatype, IBaseResource}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

private[encoders2] class SerializerBuilderVisitor(expression: Expression, val dataTypeMappings: DataTypeMappings) extends
  SchemaVisitorWithTypeMappings[Expression, ExpressionWithName] {

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaVisitorEx[Expression, ExpressionWithName], BaseRuntimeElementCompositeDefinition[_]) => Expression): Seq[ExpressionWithName] = {
    // add custom encoder
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // TODO: Enable this check or implement
    //assert(customEncoder.isEmpty || !isCollection,
    //"Collections are not supported for custom encoders for: " + elementName + "-> " + elementDefinition)
    customEncoder.map(_.customSerializer2(expression))
      .getOrElse(super.buildValue(childDefinition, elementDefinition, elementName, compositeBuilder))
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                               compositeBuilder: (SchemaVisitorEx[Expression, (String, Expression)], BaseRuntimeElementCompositeDefinition[_]) => Expression): Expression = {
    MapObjects(withExpression(_).buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder),
      expression,
      objectTypeFor(childDefinition))
  }

  override def buildElement(elementName: String, elementType: Expression, definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
    // Named serializer
    (elementName, elementType)
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    dataTypeMappings.primitiveEncoderExpression(expression, primitive)
  }

  override def buildPrimitiveDatatypeNarrative: Expression = {
    dataTypeToUtf8Expr(expression)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {
    dataTypeToUtf8Expr(expression)
  }

  override def buildComposite(fields: Seq[ExpressionWithName], definition: BaseRuntimeElementCompositeDefinition[_]): Expression = {
    // TODO: Fix so that it does not traverse the type
    val structFields = dataTypeMappings.overrideCompositeExpression(expression, definition).getOrElse(fields.flatMap({ case (name, serializer) => Seq(Literal(name), serializer) }))
    val struct = CreateNamedStruct(structFields)
    If(IsNull(expression), Literal.create(null, struct.dataType), struct)
  }

  override def enterChild(childDefinition: BaseRuntimeChildDefinition): SchemaVisitorEx[Expression, ExpressionWithName] = {
    val childExpression = childDefinition match {
      // At this point we don't the actual type of the child, so get it as the general IBaseDatatype
      case _: RuntimeChildChoiceDefinition => getChildExpression(expression, childDefinition, ObjectType(classOf[IBaseDatatype]))
      case _ => getChildExpression(expression, childDefinition)
    }
    this.withExpression(childExpression)
  }

  override def enterChoiceOption(choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): SchemaVisitorEx[Expression, ExpressionWithName] = {
    val choiceChildDefinition = choiceDefinition.getChildByName(optionName)
    val optionExpression = If(InstanceOf(expression, choiceChildDefinition.getImplementingClass),
      ObjectCast(expression, ObjectType(choiceChildDefinition.getImplementingClass)),
      Literal.create(null, ObjectType(choiceChildDefinition.getImplementingClass)))
    this.withExpression(optionExpression)
  }


  def withExpression(expression: Expression): SerializerBuilderVisitor = {
    new SerializerBuilderVisitor(expression, dataTypeMappings)
  }
}

private[encoders2] object SerializerBuilderVisitor {

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

    val cls = field match {

      case resource: RuntimeChildResourceDefinition =>
        resource.getChildByName(resource.getElementName).getImplementingClass

      case block: RuntimeChildResourceBlockDefinition =>
        getSingleChild(block).getImplementingClass

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
}


class SerializerBuilder2(mappings: DataTypeMappings, fhirContext: FhirContext, maxNestingLevel: Int) {

  def buildSerializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    val fhirClass = resourceDefinition.asInstanceOf[BaseRuntimeElementDefinition[_]].getImplementingClass
    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)
    new SchemaTraversalEx[Expression, ExpressionWithName](maxNestingLevel)
      .enterResource(new SerializerBuilderVisitor(inputObject, mappings), resourceDefinition)
  }

  def buildSerializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    buildSerializer(fhirContext.getResourceDefinition(resourceClass))
  }
}
