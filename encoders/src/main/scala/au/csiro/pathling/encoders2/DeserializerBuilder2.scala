package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.ObjectCast
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.DeserializerBuilderVisitor.setterFor
import au.csiro.pathling.encoders2.SchemaTraversal.isCollection
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNotNull, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

private[encoders2] class DeserializerBuilderVisitor(val path: Option[Expression], val dataTypeMappings: DataTypeMappings, schemaConverter: SchemaConverter2,
                                                    parent: Option[DeserializerBuilderVisitor] = None)
  extends SchemaVisitorWithTypeMappings[Expression, ExpressionWithName] {


  override def aggregateChoice(choiceDefinition: RuntimeChildChoiceDefinition, optionValues: Seq[Seq[ExpressionWithName]]): Seq[ExpressionWithName] = {
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
      case ((optionName, optionDeserializer), composite) => {
        If(IsNotNull(addToPath(optionName)),
          ObjectCast(optionDeserializer, ObjectType(dataTypeMappings.baseType())),
          composite)
      }
    }
    Seq((choiceDefinition.getElementName, choiceDeserializer))
  }

  override def buildEnumDatatype(enumDefinition: RuntimePrimitiveDatatypeDefinition, enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): Expression = {

    // TODO: Unify please
    // Only apply the special case for non list
    if (enumChildDefinition.getMax == 1) {

      // need to manually expand context here
      def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

      // Get the value and initialize the instance.
      // expandPath always returns Some
      val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

      val enumFactory = Class.forName(enumChildDefinition.getBoundEnumType.getName + "EnumFactory")

      // Creates a new enum factory instance for each invocation, but this is cheap
      // on modern JVMs and probably more efficient than attempting to pool the underlying
      // FHIR enum factory ourselves.
      val factoryInstance = NewInstance(enumFactory, Nil, false, ObjectType(enumFactory), None)

      Invoke(factoryInstance, "fromCode",
        ObjectType(enumChildDefinition.getBoundEnumType),
        List(utfToString))
    } else {
      super.buildEnumDatatype(enumDefinition, enumChildDefinition)
    }
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaVisitor[Expression, ExpressionWithName], BaseRuntimeElementCompositeDefinition[_]) => Expression): Seq[ExpressionWithName] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // NOTE: We need to pass the parent's addToPath to custom encoder, so that it can
    // access multiple elements of the parent composite
    customEncoder.map(_.customDeserializer2(parent.get.addToPath, isCollection(childDefinition)))
      .getOrElse(super.buildValue(childDefinition, elementDefinition, elementName, compositeBuilder))
  }

  override def enterChoiceOption(choiceDefinition: RuntimeChildChoiceDefinition, optionName: String): SchemaVisitor[Expression, ExpressionWithName] = {
    expandWithName(optionName)
  }

  override def enterChild(childDefinition: BaseRuntimeChildDefinition): SchemaVisitor[Expression, (String, Expression)] = {
    childDefinition match {
      case _: RuntimeChildChoiceDefinition => this
      case _ => expandWithName(childDefinition.getElementName)
    }
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                               compositeBuilder: (SchemaVisitor[Expression, ExpressionWithName], BaseRuntimeElementCompositeDefinition[_]) => Expression): Expression = {

    def elementMapper(expression: Expression): Expression = {
      withPath(Some(expression)).buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    }

    assert(path.isDefined, "We expect a non-empty path here")
    val elementType: DataType = getSqlDatatypeFor(elementDefinition)
    val arrayExpression = Invoke(
      MapObjects(elementMapper,
        path.get,
        elementType),
      "array",
      ObjectType(classOf[Array[Any]]))
    StaticInvoke(
      classOf[java.util.Arrays],
      ObjectType(classOf[java.util.List[_]]),
      "asList",
      arrayExpression :: Nil)
  }

  override def buildElement(elementName: String, elementType: Expression, definition: BaseRuntimeElementDefinition[_]): ExpressionWithName = {
    (elementName, elementType)
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): Expression = {
    dataTypeMappings.primitiveDecoderExpression(primitive.getImplementingClass, path)
  }

  override def buildPrimitiveDatatypeNarrative: Expression = ???

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): Expression = {

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

  override def buildComposite(fields: Seq[ExpressionWithName], definition: BaseRuntimeElementCompositeDefinition[_]): Expression = {
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

  private def withPath(path: Option[Expression]): DeserializerBuilderVisitor = {
    new DeserializerBuilderVisitor(path, dataTypeMappings, schemaConverter, Some(this))
  }

  private def expandWithName(name: String): DeserializerBuilderVisitor = {
    withPath(Some(addToPath(name)))
  }

  private def addToPath(name: String): Expression = {
    path.map(UnresolvedExtractValue(_, expressions.Literal(name)))
      .getOrElse(UnresolvedAttribute(name))
  }

}

private[encoders2] object DeserializerBuilderVisitor {

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

class DeserializerBuilder2(fhirContext: FhirContext, mappings: DataTypeMappings, maxNestingLevel: Int,
                           schemaConverter: SchemaConverter2) {

  def buildDeserializer[T <: IBaseResource](resourceClass: Class[T]): Expression = {
    buildDeserializer(fhirContext.getResourceDefinition(resourceClass))
  }

  def buildDeserializer(resourceDefinition: RuntimeResourceDefinition): Expression = {
    new SchemaTraversal[Expression, ExpressionWithName](maxNestingLevel)
      .enterResource(new DeserializerBuilderVisitor(None, mappings, schemaConverter), resourceDefinition)
  }
}