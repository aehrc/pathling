package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.DeserializerBuilder2.{expandPath, setterFor}
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNull}
import org.apache.spark.sql.types.ObjectType

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

//
// Add support for enums and "bound" things
//
class DeserializerBuilder2(mappings: DataTypeMappings, fhirContext: FhirContext, maxNestingLevel: Int) extends
  SchemaTraversal[Expression, ExpressionWithName, Option[Expression]](fhirContext, maxNestingLevel) {

  override def buildComposite(ctx: Option[Expression], fields: Seq[(String, Expression)], definition: BaseRuntimeElementCompositeDefinition[_]): Expression = {

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

    // TODO: Change when addding support for contained resources
    val result = bean
    //
    //    // Deserialize any Contained resources to the new Object through successive calls
    //    // to 'addContained'.
    //    val result = contained.foldLeft(bean)((value, containedResource) => {
    //
    //      Invoke(value,
    //        "addContained",
    //        ObjectType(definition.getImplementingClass),
    //        compositeToDeserializer(containedResource,
    //          Some(UnresolvedAttribute("contained." + containedResource.getName))) :: Nil)
    //    })

    // TODO: Check if this is correct
    // It's what the current code does but it might be wrong too, e.g. getPath may need to be called regardless
    // of the option status
    ctx.map(path =>
      expressions.If(
        IsNull(path),
        expressions.Literal.create(null, ObjectType(definition.getImplementingClass)),
        result)).getOrElse(result)
  }

  override def buildElement(elementName: String, elementType: Expression, definition: BaseRuntimeElementDefinition[_]): (String, Expression) = {
    (elementName, elementType)
  }

  override def buildPrimitiveDatatype(ctx: Option[Expression], primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    // TODO: Add custom expressions
    // This can be tricky as the context needs to be modifiable based on the name
    // as well as the name is needed to construct new context
    mappings.primitiveDecoderExpression(primitive.getImplementingClass, ctx)
  }

  override def buildPrimitiveDatatypeNarrative(ctx: Option[Expression]): Expression = ???

  override def buildPrimitiveDatatypeXhtmlHl7Org(ctx: Option[Expression], xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {

    def getPath: Expression = ctx.getOrElse(GetColumnByOrdinal(0, ObjectType(xhtmlHl7Org.getImplementingClass)))

    val newInstance = NewInstance(xhtmlHl7Org.getImplementingClass,
      Nil,
      ObjectType(xhtmlHl7Org.getImplementingClass))

    val stringValue = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    // If there is a non-null value, create and set the xhtml node.
    expressions.If(IsNull(stringValue),
      expressions.Literal.create(null, newInstance.dataType),
      InitializeJavaBean(newInstance, Map("setValueAsString" ->
        stringValue)))
  }

  override def buildArrayTransformer(arrayDefinition: BaseRuntimeChildDefinition): (Option[Expression], BaseRuntimeElementDefinition[_]) => Expression = ???

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
    // TODO: This should be unified somewhere else
    !mappings.skipField(definition, childDefinition)
  }


  override def visitElementChild(ctx: Option[Expression], childDefinition: BaseRuntimeChildDefinition): Seq[(String, Expression)] = {
    // switch the context to the child
    // traverse the expression path
    super.visitElementChild(expandPath(ctx, childDefinition.getElementName), childDefinition)
  }
}

object DeserializerBuilder2 {
  /**
   * Returns the setter for the given field name.s
   */

  private def expandPath(ctx: Option[Expression], name: String): Option[Expression] = {
    Some(ctx
      .map(UnresolvedExtractValue(_, expressions.Literal(name)))
      .getOrElse(UnresolvedAttribute(name)))
  }

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
}