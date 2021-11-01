package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders.{InstanceOf, ObjectCast}
import au.csiro.pathling.encoders2.SerializerBuilder.{getChildExpression, objectTypeFor}
import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, MapObjects}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.hl7.fhir.instance.model.api.{IBaseDatatype, IBaseResource}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

class SerializerBuilder(mappings: DataTypeMappings, fhirContext: FhirContext, maxNestingLevel: Int) extends
  SchemaTraversal[Expression, NamedSerializer, Expression](fhirContext, maxNestingLevel) {

  override def buildComposite(ctx: Expression, fields: Seq[NamedSerializer]): Expression = {
    val struct = CreateNamedStruct(fields.flatMap({ case (name, serializer) => Seq(Literal(name), serializer) }))
    If(IsNull(ctx), Literal.create(null, struct.dataType), struct)
  }

  override def buildPrimitiveDatatype(ctx: Expression, primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    mappings.primitiveEncoderExpression(ctx, primitive)
  }

  override def buildElement(elementName: String, elementType: Expression, definition: BaseRuntimeElementDefinition[_]): NamedSerializer = {
    // Named serializer
    (elementName, elementType)
  }

  override def buildPrimitiveDatatypeNarrative: Expression = Literal(1)

  override def buildPrimitiveDatatypeXhtmlHl7Org: Expression = Literal(1)

  override def buildArrayTransformer(arrayDefinition: BaseRuntimeChildDefinition): (Expression, BaseRuntimeElementDefinition[_]) => Expression = {
    (ctx, elementDefinition) => {
      MapObjects(visitElementValue(_, elementDefinition),
        ctx,
        objectTypeFor(arrayDefinition))
    }
  }

  override def visitChoiceChild(ctx: Expression, choiceDefinition: RuntimeChildChoiceDefinition): Seq[(String, Expression)] = {
    // At this point we don't the actual type of the child, so get it as the general IBaseDatatype
    super.visitChoiceChild(getChildExpression(ctx, choiceDefinition, ObjectType(classOf[IBaseDatatype])),
      choiceDefinition)
  }

  override def visitChoiceChildOption(ctx: Expression, choiceDefinition: RuntimeChildChoiceDefinition,
                                      optionName: String): Seq[(String, Expression)] = {
    val choiceChildDefinition = choiceDefinition.getChildByName(optionName)
    val elementCtx = If(InstanceOf(ctx, choiceChildDefinition.getImplementingClass),
      ObjectCast(ctx, ObjectType(choiceChildDefinition.getImplementingClass)),
      Literal.create(null, ObjectType(choiceChildDefinition.getImplementingClass)))
    super.visitChoiceChildOption(elementCtx, choiceDefinition, optionName)
  }


  override def visitElementChild(ctx: Expression, childDefinition: BaseRuntimeChildDefinition): Seq[(String, Expression)] = {
    // switch the context to the child
    // Get the field accessor
    // this needs to be different for lists (the type must be different)
    super.visitElementChild(getChildExpression(ctx, childDefinition), childDefinition)
  }

  def buildSerializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    val definition: BaseRuntimeElementCompositeDefinition[_] = fhirContext.getResourceDefinition(resourceClass)
    val fhirClass = definition.getImplementingClass
    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)
    visitResource(inputObject, resourceClass)
  }


  //  /**
  //   * Returns the accessor method for the given child field.
  //   */
  //  private def accessorFor(definition: BaseRuntimeElementDefinition[_], elementName: String): String = {
  //
  //    // Primitive single-value types typically use the Element suffix in their
  //    // accessors, with the exception of the "div" field for reasons that are not clear.
  //    if (definition.isInstanceOf[RuntimePrimitiveDatatypeDefinition] &&
  //      //TODO ??? field.getMax == 1 &&
  //      elementName != "div")
  //      "get" + elementName.capitalize + "Element"
  //    else {
  //      if (elementName.equals("class")) {
  //        "get" + elementName.capitalize + "_"
  //      } else {
  //        "get" + elementName.capitalize
  //      }
  //    }
  //  }


  //  /**
  //   * Returns the object type of the given child
  //   */
  //  private def objectTypeFor(definition: BaseRuntimeElementDefinition[_]): ObjectType = {
  //
  //    val cls = definition match {
  //
  //      case resource: RuntimeResourceDefinition =>
  //        resource.getImplementingClass
  //
  //      case block: RuntimeResourceBlockDefinition =>
  //        block.getImplementingClass
  //
  //      case composite: RuntimeCompositeDatatypeDefinition =>
  //        composite.getImplementingClass
  //
  //      case primitive: RuntimePrimitiveDatatypeDefinition =>
  //        primitive.getChildType match {
  //          case ChildTypeEnum.PRIMITIVE_DATATYPE =>
  //            primitive.getImplementingClass
  //
  //          case ChildTypeEnum.PRIMITIVE_XHTML_HL7ORG =>
  //            classOf[XhtmlNode]
  //
  //          case ChildTypeEnum.ID_DATATYPE =>
  //            primitive.getImplementingClass
  //
  //          case unsupported =>
  //            throw new IllegalArgumentException("Unsupported child primitive type: " + unsupported)
  //        }
  //    }
  //
  //    ObjectType(cls)
  //  }

}


object SerializerBuilder {

  private def getChildExpression(parentObject: Expression,
                                 childDefinition: BaseRuntimeChildDefinition, dataType: DataType): Expression = {
    Invoke(parentObject,
      accessorFor(childDefinition),
      dataType)
  }

  private def getChildExpression(parentObject: Expression,
                                 childDefinition: BaseRuntimeChildDefinition): Expression = {
    getChildExpression(parentObject, childDefinition, objectTypeFor(childDefinition))
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
